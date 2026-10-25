"""Statistics of the crawl and of the extracted products.

Crawl: pages, sizes on disk and failures from crawl_metadata.jsonl and html/.
Products: how many were extracted, how many are relevant (indexed), and how
many tokens their text has - with our own tokenizer (index terms) and with
tiktoken (cl100k_base, the BPE tokenizer of OpenAI models), if installed.

Usage: python stats.py
"""

from __future__ import annotations

import json
import os
from collections import Counter

from indexer import FIELD_WEIGHTS, is_relevant, tokenize

METADATA_FILE = "crawl_metadata.jsonl"
HTML_DIR = "html"
PRODUCTS_FILE = "products_data.jsonl"


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def main() -> None:
    pages = Counter()
    failures = Counter()
    failed_barcodes = set()
    saved_barcodes = set()
    raw_bytes = Counter()
    gzip_bytes = Counter()
    elapsed = []
    first = last = None

    with open(METADATA_FILE, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            kind = r["page_type"]
            first = first or r["fetched_at"]
            last = r["fetched_at"]
            if r["html_path"]:
                pages[kind] += 1
                raw_bytes[kind] += r["size_bytes"] or 0
                gzip_bytes[kind] += r["size_gzip_bytes"] or 0
                elapsed.append(r["elapsed_ms"])
                saved_barcodes.add(r["barcode"])
            else:
                failed_barcodes.add(r["barcode"])
                # Group by error type, e.g. "ConnectionError" rather than the full message.
                reason = r["error"] or f"HTTP {r['status']}"
                failures[reason.split(":")[0]] += 1

    disk = sum(
        os.path.getsize(os.path.join(root, name))
        for root, _, names in os.walk(HTML_DIR) for name in names
    )

    print(f"Crawl period:        {first} .. {last}")
    print(f"{'':20} {'pages':>10} {'raw HTML':>12} {'gzipped':>12}")
    for kind in sorted(pages):
        print(f"{kind:20} {pages[kind]:>10} {human(raw_bytes[kind]):>12} {human(gzip_bytes[kind]):>12}")
    print(f"{'total':20} {sum(pages.values()):>10} {human(sum(raw_bytes.values())):>12} "
          f"{human(sum(gzip_bytes.values())):>12}")
    print(f"Size on disk (html/): {human(disk)}")
    if elapsed:
        print(f"Avg fetch time:      {sum(elapsed) / len(elapsed):.0f} ms")
    print(f"Failed fetches:      {sum(failures.values())}")
    for reason, count in failures.most_common():
        print(f"  {count:>6}  {reason}")
    missing = {b for b in failed_barcodes - saved_barcodes if b}
    print(f"Products still missing (failed, never saved): {len(missing)}")

    if os.path.exists(PRODUCTS_FILE):
        product_stats()


def product_text(product: dict) -> str:
    """The searchable text of a product, every field once."""
    values = []
    for field in FIELD_WEIGHTS:
        value = product.get(field)
        if value:
            values.append(" ".join(value) if isinstance(value, list) else str(value))
    return " ".join(values)


def product_stats() -> None:
    try:
        import tiktoken
        encoding = tiktoken.get_encoding("cl100k_base")
    except ImportError:
        encoding = None

    extracted = relevant = own_tokens = bpe_tokens = 0
    with open(PRODUCTS_FILE, encoding="utf-8") as f:
        for line in f:
            product = json.loads(line)
            extracted += 1
            if not is_relevant(product):
                continue
            relevant += 1
            text = product_text(product)
            own_tokens += len(tokenize(text))
            if encoding:
                bpe_tokens += len(encoding.encode(text, disallowed_special=()))

    print(f"\nExtracted products:  {extracted}")
    print(f"Relevant products:   {relevant} ({relevant / extracted:.1%}) - have a name and "
          f"ingredients or nutrition; these are indexed")
    print(f"Tokens in relevant products (own tokenizer, index terms): {own_tokens:,}")
    if encoding:
        print(f"Tokens in relevant products (tiktoken cl100k_base):       {bpe_tokens:,}")
    else:
        print("tiktoken not installed: pip install tiktoken  (for the BPE token count)")


if __name__ == "__main__":
    main()
