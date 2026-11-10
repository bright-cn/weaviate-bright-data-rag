"""
Enterprise RAG Pipeline: Bright Data + Weaviate
================================================
Pick any topic → Bright Data finds and scrapes real articles about it →
Weaviate stores and indexes them → Ask questions, get cited answers.

Setup:
  1. pip3 install -r requirements.txt
  2. Create a .env file (see .env.example)
  3. python3 pipeline.py

Full tutorial: https://brightdata.com/blog/web-data/weaviate-bright-data-enterprise-rag
"""

import os
import sys
import time
import hashlib
import requests
import urllib3
from urllib.parse import quote
from datetime import datetime, timezone
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure, Property, DataType

urllib3.disable_warnings()
load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────

COHERE_API_KEY = os.getenv("COHERE_API_KEY")
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
BD_API_TOKEN = os.getenv("BRIGHT_DATA_API_TOKEN")

COLLECTION_NAME = "WebResearch"


# ─── Helpers ─────────────────────────────────────────────────────────────────

def clean_url(url):
    """Fix nbsp artifacts in URLs (from &nbsp; encoding issues in some sites)."""
    cleaned = url.replace("nbsp", "-")
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned


def clean_generated_text(text):
    """Clean LLM-generated text for terminal display."""
    # Strip markdown bold markers (terminal can't render them)
    text = text.replace("**", "")
    # Fix nbsp artifacts (appears in URL slugs from poorly-encoded sites)
    text = text.replace("nbsp", "-")
    while "--" in text:
        text = text.replace("--", "-")
    return text


# ─── Validation ───────────────────────────────────────────────────────────────

def validate_env():
    """Check all required environment variables are set."""
    missing = []
    if not BD_API_TOKEN:
        missing.append("BRIGHT_DATA_API_TOKEN")
    if not COHERE_API_KEY:
        missing.append("COHERE_API_KEY")
    if not WEAVIATE_URL:
        missing.append("WEAVIATE_URL")
    if not WEAVIATE_API_KEY:
        missing.append("WEAVIATE_API_KEY")

    if missing:
        print("ERROR: Missing environment variables in .env file:")
        for var in missing:
            print(f"  - {var}")
        print("\nCreate a .env file in this directory with:")
        print("  BRIGHT_DATA_API_TOKEN=your_token")
        print("  COHERE_API_KEY=your_key")
        print("  WEAVIATE_URL=https://your-cluster.weaviate.cloud")
        print("  WEAVIATE_API_KEY=your_key")
        print("\nSee the blog post for how to get each key (all free to start).")
        sys.exit(1)


# ─── Bright Data: Auto-discover Credentials ──────────────────────────────────

def discover_bright_data_credentials():
    """
    Automatically discover Bright Data proxy credentials from the API key.
    This works for ANY Bright Data account — no hardcoded values needed.

    Returns: (customer_id, unlocker_zone, unlocker_password, serp_zone, serp_password)
    """
    headers = {"Authorization": f"Bearer {BD_API_TOKEN}"}

    # 1. Get active zones
    print("  Discovering Bright Data zones...", end=" ", flush=True)
    r = requests.get("https://api.brightdata.com/zone/get_active_zones", headers=headers)
    if r.status_code != 200:
        print(f"FAILED (HTTP {r.status_code})")
        print("  Check your BRIGHT_DATA_API_TOKEN in .env")
        sys.exit(1)

    zones = r.json()

    # Pick the first zone of each type (if you have multiple, set the name explicitly)
    zone_names = {}
    for z in zones:
        if z["type"] not in zone_names:
            zone_names[z["type"]] = z["name"]

    # Find Web Unlocker zone
    unlocker_zone = zone_names.get("unblocker")
    if not unlocker_zone:
        print("FAILED")
        print("  No Web Unlocker zone found. Create one at brightdata.com/cp/zones")
        sys.exit(1)

    # Find SERP zone (required for topic-driven discovery)
    serp_zone = zone_names.get("serp")
    if not serp_zone:
        print("FAILED")
        print("  No SERP API zone found. The pipeline needs SERP API to find articles")
        print("  about your topic. Create a SERP zone at brightdata.com/cp/zones")
        sys.exit(1)

    print(f"OK (unlocker: {unlocker_zone}, serp: {serp_zone})")

    # 2. Get zone passwords
    unlocker_pwd_r = requests.get(
        f"https://api.brightdata.com/zone/passwords?zone={unlocker_zone}", headers=headers
    )
    unlocker_password = unlocker_pwd_r.json()["passwords"][0]

    serp_pwd_r = requests.get(
        f"https://api.brightdata.com/zone/passwords?zone={serp_zone}", headers=headers
    )
    serp_password = serp_pwd_r.json()["passwords"][0]

    # 3. Get customer ID (the cost endpoint returns {customer_id: cost_data})
    cost_r = requests.get(
        f"https://api.brightdata.com/zone/cost?zone={unlocker_zone}", headers=headers
    )
    customer_id = list(cost_r.json().keys())[0]
    print(f"  Customer ID: {customer_id}")

    return customer_id, unlocker_zone, unlocker_password, serp_zone, serp_password


# ─── Phase 1: Data Collection with Bright Data ───────────────────────────────

def get_bd_proxy(customer_id, zone, password):
    """Build Bright Data proxy URL."""
    proxy = f"http://brd-customer-{customer_id}-zone-{zone}:{password}@brd.superproxy.io:33335"
    return {"http": proxy, "https": proxy}


def search_serp(query, customer_id, zone, password, num=10):
    """Search Google via Bright Data SERP API and return organic results."""
    proxies = get_bd_proxy(customer_id, zone, password)
    search_url = f"https://www.google.com/search?q={quote(query)}&brd_json=1&num={num}"

    try:
        # verify=False bypasses SSL verification for the BD proxy.
        # For production, install the Bright Data CA certificate instead:
        # https://docs.brightdata.com/general/account/ssl-certificate
        response = requests.get(search_url, proxies=proxies, timeout=30, verify=False)
        if response.status_code == 200:
            data = response.json()
            return [
                {
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "description": item.get("description", ""),
                }
                for item in data.get("organic", [])
            ]
    except Exception as e:
        print(f"SERP error: {str(e)[:60]}", end=" ", flush=True)
    return []


def find_articles_for_topic(topic, customer_id, serp_zone, serp_pwd):
    """Use Bright Data SERP API to find real article URLs about a topic."""
    # Search with 2 different query angles to get diverse results
    search_queries = [
        f"{topic} latest news and trends",
        f"{topic} in-depth analysis guide",
    ]

    # Domains to skip (not article content)
    skip_domains = {
        "youtube.com", "twitter.com", "x.com", "facebook.com", "instagram.com",
        "reddit.com", "linkedin.com", "wikipedia.org", "amazon.com", "tiktok.com",
    }
    skip_extensions = (".pdf", ".doc", ".ppt", ".xls", ".zip", ".mp4", ".mp3")

    all_urls = []
    seen_domains = set()
    serp_docs = []

    print(f"\n  [SERP API] Finding articles about '{topic}'...")

    for query in search_queries:
        print(f"    Searching: '{query}'... ", end="", flush=True)
        results = search_serp(query, customer_id, serp_zone, serp_pwd, num=10)

        if results:
            print(f"OK ({len(results)} results)")

            # Save SERP titles + descriptions as a document so the LLM can
            # reference article summaries even if the full scrape fails
            serp_text = f"Google search results for: {query}\n\n"
            for r in results:
                serp_text += f"Title: {r['title']}\nURL: {r['url']}\n"
                serp_text += f"Summary: {r['description']}\n\n"
            serp_docs.append({
                "url": f"https://google.com/search?q={quote(query)}",
                "html": serp_text,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "is_serp": True,
            })

            # Extract article URLs (filter out junk)
            for r in results:
                url = r.get("url", "")
                if not url:
                    continue

                # Skip non-article URLs
                domain = url.split("/")[2] if "://" in url else ""
                base_domain = ".".join(domain.split(".")[-2:])

                if base_domain in skip_domains:
                    continue
                if any(url.lower().endswith(ext) for ext in skip_extensions):
                    continue
                if base_domain in seen_domains:
                    continue  # One article per domain for diversity

                seen_domains.add(base_domain)
                all_urls.append(url)
        else:
            print("FAILED (skipping)")

    # Take the top 6 URLs
    urls_to_scrape = all_urls[:6]
    print(f"\n  Found {len(urls_to_scrape)} article URLs to scrape:")
    for url in urls_to_scrape:
        domain = url.split("/")[2] if "://" in url else url
        print(f"    - {domain}")

    return urls_to_scrape, serp_docs


def scrape_url(url, customer_id, zone, password, retries=2):
    """Scrape a single URL using Bright Data Web Unlocker with retry."""
    proxies = get_bd_proxy(customer_id, zone, password)
    # No custom headers needed: Web Unlocker manages User-Agent,
    # cookies, and fingerprints automatically for optimal success rates.

    for attempt in range(retries + 1):
        try:
            # verify=False bypasses SSL verification for the BD proxy.
            # For production, install the Bright Data CA certificate instead:
            # https://docs.brightdata.com/general/account/ssl-certificate
            response = requests.get(
                url, proxies=proxies, timeout=60, verify=False
            )
            if response.status_code == 200:
                return {
                    "url": url,
                    "html": response.text,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                print(f"HTTP {response.status_code}", end=" → ", flush=True)
        except Exception as e:
            print(f"Error: {str(e)[:60]}", end=" → ", flush=True)

        if attempt < retries:
            time.sleep(2)

    return None


def collect_data(topic, customer_id, unlocker_zone, unlocker_pwd, serp_zone, serp_pwd):
    """Find articles about the topic via SERP, then scrape them with Web Unlocker."""
    documents = []

    # 1. Use SERP API to find article URLs
    urls_to_scrape, serp_docs = find_articles_for_topic(
        topic, customer_id, serp_zone, serp_pwd
    )

    if not urls_to_scrape:
        print("  ERROR: No article URLs found. Try a different topic.")
        return []

    # 2. Scrape the found articles with Web Unlocker
    print(f"\n  [Web Unlocker] Scraping {len(urls_to_scrape)} articles...")
    for i, url in enumerate(urls_to_scrape):
        domain = url.split("/")[2] if "://" in url else url
        print(f"    ({i+1}/{len(urls_to_scrape)}) {domain}... ", end="", flush=True)
        result = scrape_url(url, customer_id, unlocker_zone, unlocker_pwd)
        if result:
            documents.append(result)
            print(f"OK ({len(result['html']):,} chars)")
        else:
            print("FAILED (skipping)")

    # 3. Add SERP results as additional documents
    documents.extend(serp_docs)

    return documents


# ─── Phase 2: Processing ─────────────────────────────────────────────────────

def clean_html(html, is_serp=False):
    """Convert HTML to clean text. SERP results are already clean."""
    if is_serp:
        return html

    soup = BeautifulSoup(html, "html.parser")

    # Remove noise elements
    for tag in soup(["nav", "footer", "header", "script", "style", "aside",
                     "iframe", "noscript", "svg", "form", "button"]):
        tag.decompose()

    # Remove common ad/cookie/popup containers
    for selector in [".ad", ".ads", ".cookie", ".popup", ".modal", ".sidebar",
                     "#cookie-banner", "#ad-container", "[role='banner']",
                     "[role='navigation']", "[role='complementary']"]:
        for el in soup.select(selector):
            el.decompose()

    text = soup.get_text(separator="\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines)


def chunk_text(text, chunk_size=500, chunk_overlap=50):
    """Split text into overlapping chunks, breaking at sentence boundaries."""
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size

        # Try to break at a sentence boundary
        if end < len(text):
            for sep in [". ", ".\n", "\n\n", "\n", " "]:
                last_sep = text[max(start, end - 100):end].rfind(sep)
                if last_sep != -1:
                    end = max(start, end - 100) + last_sep + len(sep)
                    break

        chunk = text[start:end].strip()
        if chunk and len(chunk) > 50:  # Skip tiny fragments
            chunks.append(chunk)
        start = end - chunk_overlap

    return chunks


def process_documents(documents):
    """Clean, chunk, and add metadata to all collected documents."""
    all_chunks = []

    for doc in documents:
        is_serp = doc.get("is_serp", False)
        clean_text = clean_html(doc["html"], is_serp=is_serp)

        if len(clean_text) < 100:
            print(f"    Skipping {doc['url']}: too short ({len(clean_text)} chars)")
            continue

        chunks = chunk_text(clean_text)
        domain = doc["url"].split("/")[2] if "://" in doc["url"] else "unknown"

        for i, chunk in enumerate(chunks):
            all_chunks.append({
                "text": chunk,
                "source_url": doc["url"],
                "source_domain": domain,
                "scraped_at": doc["scraped_at"],
                "chunk_index": i,
                "total_chunks": len(chunks),
                "content_hash": hashlib.md5(chunk.encode()).hexdigest(),
                "content_type": "serp_result" if is_serp else "article",
            })

    return all_chunks


# ─── Phase 3: Weaviate Storage ───────────────────────────────────────────────

def connect_weaviate():
    """Connect to Weaviate Cloud with extended timeout."""
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL,
        auth_credentials=Auth.api_key(WEAVIATE_API_KEY),
        headers={"X-Cohere-Api-Key": COHERE_API_KEY},
        additional_config=weaviate.classes.init.AdditionalConfig(
            timeout=weaviate.classes.init.Timeout(init=30, query=60, insert=120),
        ),
        skip_init_checks=True,
    )

    if not client.is_ready():
        print("  ERROR: Weaviate cluster is not ready.")
        print("  Check your WEAVIATE_URL and WEAVIATE_API_KEY in .env")
        print("  Make sure your sandbox cluster is running at console.weaviate.cloud")
        sys.exit(1)

    return client


def setup_collection(client):
    """Create the collection with hybrid search + generative config."""
    if client.collections.exists(COLLECTION_NAME):
        client.collections.delete(COLLECTION_NAME)
        print(f"    Deleted existing '{COLLECTION_NAME}' collection")

    client.collections.create(
        name=COLLECTION_NAME,
        description="Web articles scraped via Bright Data for RAG",

        # Cohere embed-v4.0 for auto-vectorization (Trial plan)
        vector_config=Configure.Vectors.text2vec_cohere(
            model="embed-v4.0",
        ),

        # Cohere command-a for RAG generation (Trial plan)
        generative_config=Configure.Generative.cohere(
            model="command-a-03-2025",
        ),

        properties=[
            Property(name="text", data_type=DataType.TEXT,
                     description="The chunk text content"),
            Property(name="source_url", data_type=DataType.TEXT,
                     skip_vectorization=True),
            Property(name="source_domain", data_type=DataType.TEXT,
                     skip_vectorization=True),
            Property(name="scraped_at", data_type=DataType.TEXT,
                     skip_vectorization=True),
            Property(name="chunk_index", data_type=DataType.INT,
                     skip_vectorization=True),
            Property(name="total_chunks", data_type=DataType.INT,
                     skip_vectorization=True),
            Property(name="content_hash", data_type=DataType.TEXT,
                     skip_vectorization=True),
            Property(name="content_type", data_type=DataType.TEXT,
                     skip_vectorization=True),
        ],
    )
    print(f"    Created '{COLLECTION_NAME}' collection")


def store_chunks(client, chunks):
    """Batch import chunks into Weaviate (auto-vectorized via Cohere)."""
    collection = client.collections.use(COLLECTION_NAME)

    with collection.batch.fixed_size(batch_size=50) as batch:
        for chunk in chunks:
            batch.add_object(properties=chunk)

    failed = len(collection.batch.failed_objects) if collection.batch.failed_objects else 0

    if failed > 0:
        print(f"    First error: {collection.batch.failed_objects[0].message[:120]}")

    return failed


# ─── Phase 4: RAG Queries ────────────────────────────────────────────────────

def rag_query(client, question, alpha=0.7, limit=5):
    """Run a RAG query using Weaviate hybrid search + generative AI."""
    collection = client.collections.use(COLLECTION_NAME)

    response = collection.generate.hybrid(
        query=question,
        alpha=alpha,  # 0.7 = 70% semantic, 30% keyword
        limit=limit,
        grouped_task=f"""Based on the retrieved documents below, answer this question:
"{question}"

Instructions:
- Provide a clear, comprehensive answer
- Cite the source URL for each key claim
- If information seems outdated or conflicting, note it
- Keep the answer concise but informative (2-4 paragraphs)""",
    )

    print(f"\n  Q: {question}")
    print(f"  {'─' * 60}")

    if response.generated:
        print(f"  A: {clean_generated_text(response.generated)}")
    else:
        print("  A: (No response generated — check your Cohere API key)")

    # Separate article sources from SERP summary chunks
    article_sources = []
    serp_sources = []
    seen_urls = set()
    for obj in response.objects:
        url = obj.properties.get("source_url", "unknown")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        content_type = obj.properties.get("content_type", "")
        domain = obj.properties.get("source_domain", "")
        if content_type == "serp_result":
            serp_sources.append((domain, url))
        else:
            article_sources.append((domain, clean_url(url)))

    print(f"\n  Sources ({len(response.objects)} chunks retrieved):")
    for domain, url in article_sources:
        print(f"    - [{domain}] {url}")
    if not article_sources and serp_sources:
        print("    (Based on SERP summaries — no article chunks matched)")

    return response


# ─── Main Pipeline ────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  RAG Pipeline: Bright Data + Weaviate")
    print("  Scrape any topic → Build a knowledge base → Ask questions")
    print("=" * 65)

    # ── Validate environment ──
    validate_env()

    # ── Ask the user for a topic ──
    print()
    try:
        topic = input("  What topic do you want to research? ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n  Goodbye!")
        return

    if not topic:
        print("  No topic entered. Exiting.")
        return

    print(f"\n  Building a RAG knowledge base about: \"{topic}\"")

    # ── Discover Bright Data credentials automatically ──
    print("\n[SETUP] Connecting to Bright Data...")
    cust_id, unlocker_zone, unlocker_pwd, serp_zone, serp_pwd = (
        discover_bright_data_credentials()
    )

    # ── Step 1: Find and scrape articles about the topic ──
    print(f"\n[STEP 1/4] Finding and scraping articles about \"{topic}\"...")
    documents = collect_data(
        topic, cust_id, unlocker_zone, unlocker_pwd, serp_zone, serp_pwd
    )
    print(f"\n  Total documents collected: {len(documents)}")

    if not documents:
        print("  ERROR: No documents collected. Try a different topic.")
        return

    # ── Step 2: Process and chunk ──
    print("\n[STEP 2/4] Processing and chunking documents...")
    chunks = process_documents(documents)
    print(f"  Created {len(chunks)} chunks from {len(documents)} documents")

    if not chunks:
        print("  ERROR: No chunks created. Documents may be too short.")
        return

    # ── Step 3: Store in Weaviate ──
    print("\n[STEP 3/4] Storing in Weaviate (embedding + indexing)...")
    print("  Connecting to Weaviate Cloud...", end=" ", flush=True)
    client = connect_weaviate()
    print("OK")

    print("  Setting up collection...")
    setup_collection(client)

    print(f"  Importing {len(chunks)} chunks (auto-vectorizing via Cohere)...")
    failed = store_chunks(client, chunks)
    print(f"  Imported: {len(chunks) - failed} success, {failed} failed")

    # Verify count
    collection = client.collections.use(COLLECTION_NAME)
    count = collection.aggregate.over_all(total_count=True).total_count
    print(f"  Total objects in Weaviate: {count}")

    # ── Step 4: Demo queries + Interactive mode ──
    print(f"\n[STEP 4/4] RAG queries about \"{topic}\"...")
    print("=" * 65)

    # Run 3 auto-generated demo queries
    demo_queries = [
        f"What are the latest developments and trends in {topic}?",
        f"What are the biggest challenges and risks in {topic}?",
        f"What is the future outlook for {topic}?",
    ]

    for question in demo_queries:
        rag_query(client, question)
        print()

    # ── Summary ──
    print("=" * 65)
    print("  Pipeline complete!")
    print(f"  Topic: \"{topic}\"")
    print(f"  - Scraped {len(documents)} sources via Bright Data")
    print(f"  - Stored {count} chunks in Weaviate")
    print(f"  - Ran {len(demo_queries)} demo RAG queries")
    print("=" * 65)

    # ── Interactive Mode ──
    print(f"\n  Your knowledge base about \"{topic}\" is ready!")
    print("  Ask anything. Type 'quit' to exit.\n")

    while True:
        try:
            user_question = input("  Your question: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            break

        if not user_question:
            continue
        if user_question.lower() in ("quit", "exit", "q"):
            print("  Goodbye!")
            break

        rag_query(client, user_question)
        print()

    client.close()


if __name__ == "__main__":
    main()
