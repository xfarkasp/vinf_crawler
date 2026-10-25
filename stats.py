"""Crawl statistics from crawl_metadata.jsonl and the html/ directory.

Usage: python stats.py
"""

from __future__ import annotations

import json
import os
from collections import Counter

METADATA_FILE = "crawl_metadata.jsonl"
HTML_DIR = "html"


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def main() -> None:
    pages = Counter()
    failures = Counter()
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
            else:
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


if __name__ == "__main__":
    main()
