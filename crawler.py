"""OpenFoodFacts HTML crawler (University IR project).

Downloads product pages from world.openfoodfacts.org as raw HTML - no API,
JSON dumps or FTP. The crawler only downloads and stores pages; extracting
product data from the stored HTML is done separately by extractor.py.

Outputs (in the current directory):
  html/listings/<page>.html.gz                 raw listing pages
  html/products/<shard>/<barcode>.html.gz      raw product pages
  crawl_metadata.jsonl                         one record per HTTP fetch
  visited_urls.txt                             finished URLs (for resuming)
  crawler.log                                  log
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://world.openfoodfacts.org"
USER_AGENT = "University IR Project Crawler / Contact: xfarkasp@stuba.sk"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en",
}

# All /category/... and /brand/... pages redirect to /facets/..., which
# robots.txt disallows. The paginated home page (/, /2, /3, ...) lists every
# product and is allowed, so discovery starts there.
SEED_URLS = [f"{BASE_URL}/"]

# Listing pages beyond /10 require a logged-in account. Log in on the website
# in your browser and paste the Cookie header value into this file (or set the
# OFF_COOKIE environment variable). Keep it private - it is your login.
COOKIE_FILE = "off_cookie.txt"

HTML_DIR = "html"
METADATA_FILE = "crawl_metadata.jsonl"
VISITED_FILE = "visited_urls.txt"
LOG_FILE = "crawler.log"

CRAWL_DELAY = 1.0  # seconds between requests
REQUEST_TIMEOUT = 20  # seconds
MAX_RETRIES = 3
MAX_REDIRECTS = 5
RETRY_BACKOFF = 2.0  # extra seconds per retry attempt (2s, 4s)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("off_crawler")

# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------

_session = requests.Session()
_session.headers.update(HEADERS)


def _load_cookie() -> None:
    """Attach the logged-in session cookie, if one is configured."""
    cookie = os.environ.get("OFF_COOKIE")
    if not cookie:
        try:
            with open(COOKIE_FILE, encoding="utf-8") as f:
                cookie = f.read().strip()
        except FileNotFoundError:
            pass
    if cookie:
        if cookie.lower().startswith("cookie:"):
            cookie = cookie[len("cookie:"):].strip()
        _session.headers["Cookie"] = cookie
        log.info("Using logged-in session cookie")
    else:
        log.info("No session cookie configured; listing pages beyond /10 will be refused")


_load_cookie()

_robots = robotparser.RobotFileParser()
_robots_loaded = False
_last_request_time = 0.0


def _load_robots() -> None:
    """Download and parse robots.txt once (using our own User-Agent)."""
    global _robots_loaded, CRAWL_DELAY
    robots_url = f"{BASE_URL}/robots.txt"
    try:
        _wait_for_rate_limit()
        resp = _session.get(robots_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        _robots.parse(resp.text.splitlines())
        # Honour a Crawl-delay directive if it is stricter than ours.
        delay = _robots.crawl_delay(USER_AGENT)
        if delay and delay > CRAWL_DELAY:
            CRAWL_DELAY = float(delay)
            log.info("robots.txt requests crawl delay of %.1fs", CRAWL_DELAY)
        log.info("Loaded robots.txt from %s", robots_url)
    except requests.RequestException as exc:
        log.warning("Could not load robots.txt (%s); assuming allow-all", exc)
        _robots.parse([])
    _robots_loaded = True


def _wait_for_rate_limit() -> None:
    """Sleep so that consecutive requests are at least CRAWL_DELAY apart."""
    global _last_request_time
    elapsed = time.monotonic() - _last_request_time
    if elapsed < CRAWL_DELAY:
        time.sleep(CRAWL_DELAY - elapsed)
    _last_request_time = time.monotonic()


def is_allowed(url: str) -> bool:
    """Check whether robots.txt allows our User-Agent to fetch `url`."""
    if not _robots_loaded:
        _load_robots()
    return _robots.can_fetch(USER_AGENT, url)


@dataclass
class FetchResult:
    """Outcome of fetching one URL; `html` is None on failure."""
    url: str
    final_url: Optional[str] = None
    status: Optional[int] = None
    html: Optional[str] = None
    content_type: Optional[str] = None
    attempts: int = 0
    elapsed_ms: int = 0
    error: Optional[str] = None


def _get_following_allowed_redirects(url: str, result: FetchResult) -> Optional[requests.Response]:
    """GET `url`, following redirects manually so each hop is checked against
    robots.txt (e.g. /category/... now redirects to the disallowed /facets/...).
    Returns None if a redirect leads somewhere we may not crawl."""
    for _ in range(MAX_REDIRECTS + 1):
        resp = _session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        if not resp.is_redirect:
            result.final_url = url
            return resp
        # requests decodes headers as latin-1; OFF sends UTF-8 (non-ASCII slugs).
        location = resp.headers.get("Location", "")
        try:
            location = location.encode("latin-1").decode("utf-8")
        except UnicodeError:
            pass
        target = urljoin(url, location)
        if urlparse(target).netloc != urlparse(BASE_URL).netloc or not is_allowed(target):
            result.error = f"redirect to disallowed URL {target}"
            log.info("Redirect %s -> %s not allowed, skipping", url, target)
            return None
        url = target
        _wait_for_rate_limit()
    result.error = "too many redirects"
    log.warning("Too many redirects for %s", url)
    return None


def fetch(url: str) -> FetchResult:
    """Fetch a page. Never raises; failures are reported in the result.

    - Respects robots.txt and the crawl delay.
    - Retries up to MAX_RETRIES times on 5xx, 429, timeouts and connection errors.
    """
    result = FetchResult(url=url)
    if urlparse(url).netloc != urlparse(BASE_URL).netloc:
        result.error = "off-site URL"
        return result
    if not is_allowed(url):
        result.error = "disallowed by robots.txt"
        log.info("Blocked by robots.txt: %s", url)
        return result

    started = time.monotonic()
    for attempt in range(1, MAX_RETRIES + 1):
        result.attempts = attempt
        _wait_for_rate_limit()
        try:
            resp = _get_following_allowed_redirects(url, result)
        except (requests.Timeout, requests.ConnectionError) as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            log.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, url, exc)
        except requests.RequestException as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            log.error("Request error for %s: %s", url, exc)
            break
        else:
            if resp is None:
                break
            result.status = resp.status_code
            result.content_type = resp.headers.get("Content-Type")
            if resp.status_code == 200:
                resp.encoding = resp.encoding or "utf-8"
                result.html = resp.text
                result.error = None
                break
            result.error = f"HTTP {resp.status_code}"
            if resp.status_code == 429 or resp.status_code >= 500:
                log.warning("Attempt %d/%d: HTTP %d for %s", attempt, MAX_RETRIES, resp.status_code, url)
            else:
                # 404 and other client errors will not get better by retrying.
                log.warning("HTTP %d for %s, not retrying", resp.status_code, url)
                if resp.status_code == 401:
                    log.warning(
                        "401 = page requires a logged-in account; check %s / OFF_COOKIE "
                        "(the cookie may be missing or expired)", COOKIE_FILE,
                    )
                break

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)
    else:
        log.error("Giving up on %s after %d attempts", url, MAX_RETRIES)

    result.elapsed_ms = int((time.monotonic() - started) * 1000)
    return result


# ---------------------------------------------------------------------------
# Link extraction (regex only)
# ---------------------------------------------------------------------------

# Product URLs look like /product/<barcode>/<slug>. Listing pages render their
# product grid with JavaScript from data embedded in the HTML, so the links are
# found with a regex over the raw HTML rather than via <a href> tags.
PRODUCT_LINK_RE = re.compile(r"/product/(\d{4,})(?:/([A-Za-z0-9_.%-]*))?")
# Pagination: <a href="/2" rel="next$nofollow">Next</a>
NEXT_PAGE_RE = re.compile(r'<a\s[^>]*href="([^"]+)"[^>]*>\s*Next\s*</a>', re.IGNORECASE)


def extract_product_links(html: str) -> list:
    """Return the product URLs found on a listing page (one per barcode)."""
    slugs = {}  # barcode -> slug; keep the URL with a slug to avoid a redirect
    for code, slug in PRODUCT_LINK_RE.findall(html):
        if not slugs.get(code):
            slugs[code] = slug
    return [
        f"{BASE_URL}/product/{code}/{slug}" if slug else f"{BASE_URL}/product/{code}"
        for code, slug in slugs.items()
    ]


def extract_next_page(html: str, current_url: str) -> Optional[str]:
    """Return the URL of the next listing page, or None on the last page."""
    match = NEXT_PAGE_RE.search(html)
    return urljoin(current_url, match.group(1)) if match else None


def guess_next_page(url: str) -> str:
    """Compute the next listing page URL without fetching (/ -> /2, /N -> /N+1).

    Used to skip listing pages that were fully processed in an earlier run.
    """
    path = urlparse(url).path.rstrip("/")
    match = re.search(r"/(\d+)$", path)
    if match:
        return f"{BASE_URL}{path[: match.start()]}/{int(match.group(1)) + 1}"
    return f"{BASE_URL}{path}/2"


def product_code(url: str) -> Optional[str]:
    match = PRODUCT_LINK_RE.search(url)
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def html_path_for(url: str) -> str:
    """Where the raw HTML of `url` is stored. Product pages are sharded by the
    first 3 digits of the barcode so no directory gets millions of files."""
    code = product_code(url)
    if code:
        return os.path.join(HTML_DIR, "products", code[:3], f"{code}.html.gz")
    page = urlparse(url).path.strip("/").replace("/", "_") or "1"
    return os.path.join(HTML_DIR, "listings", f"{page}.html.gz")


def save_html(html: str, url: str) -> tuple:
    """Store the raw HTML gzipped. Returns (path, raw size, gzipped size)."""
    path = html_path_for(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    raw = html.encode("utf-8")
    with gzip.open(path, "wb") as f:
        f.write(raw)
    return path, len(raw), os.path.getsize(path)


def save_metadata(result: FetchResult, page_type: str, html_path: Optional[str] = None,
                  size_bytes: Optional[int] = None, size_gzip_bytes: Optional[int] = None,
                  links_found: Optional[int] = None) -> None:
    """Append one crawl-metadata record per fetch (successful or not)."""
    record = {
        "url": result.url,
        "final_url": result.final_url,
        "page_type": page_type,
        "barcode": product_code(result.url) if page_type == "product" else None,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": result.status,
        "content_type": result.content_type,
        "attempts": result.attempts,
        "elapsed_ms": result.elapsed_ms,
        "size_bytes": size_bytes,
        "size_gzip_bytes": size_gzip_bytes,
        "html_path": html_path,
        "links_found": links_found,
        "error": result.error,
    }
    with open(METADATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_visited(path: str = VISITED_FILE) -> set:
    """Load URLs finished in earlier runs (product and listing pages)."""
    try:
        with open(path, encoding="utf-8") as f:
            visited = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        visited = set()
    log.info("Loaded %d visited URLs from %s", len(visited), path)
    return visited


def mark_visited(url: str, path: str = VISITED_FILE) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(url + "\n")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def crawl(max_products: Optional[int] = None) -> None:
    """Breadth-first crawl: listing page -> its product pages -> next listing page."""
    visited = load_visited()
    visited_codes = {c for c in map(product_code, visited) if c}
    listing_queue = deque(SEED_URLS)
    saved = 0

    while listing_queue:
        page_url = listing_queue.popleft()

        # A listing page is only marked visited once all its products are
        # done, so on restart it can be skipped without fetching it.
        if page_url in visited:
            listing_queue.append(guess_next_page(page_url))
            continue

        log.info("Listing page %s", page_url)
        result = fetch(page_url)
        if result.html is None:
            save_metadata(result, "listing")
            log.warning("Could not fetch listing page %s, stopping discovery here", page_url)
            continue

        links = extract_product_links(result.html)
        save_metadata(result, "listing", *save_html(result.html, page_url), links_found=len(links))
        product_queue = deque(u for u in links if product_code(u) not in visited_codes)
        next_url = extract_next_page(result.html, page_url)
        log.info("Found %d new products, next page: %s", len(product_queue), next_url)

        while product_queue:
            product_url = product_queue.popleft()
            result = fetch(product_url)
            if result.html is None:
                save_metadata(result, "product")
                continue  # not marked visited, so it is retried on the next run
            save_metadata(result, "product", *save_html(result.html, product_url))
            mark_visited(product_url)
            visited_codes.add(product_code(product_url))
            saved += 1
            log.info("[%d] Saved %s", saved, product_url)
            if max_products and saved >= max_products:
                log.info("Reached --max-products=%d, stopping", max_products)
                return

        mark_visited(page_url)
        visited.add(page_url)
        if next_url:
            listing_queue.append(next_url)

    log.info("Queue empty, crawl finished. Saved %d products this run.", saved)


def main() -> None:
    parser = argparse.ArgumentParser(description="OpenFoodFacts HTML crawler")
    parser.add_argument(
        "--max-products", type=int, default=None,
        help="stop after saving this many products (default: run until the queue is empty)",
    )
    args = parser.parse_args()
    try:
        crawl(args.max_products)
    except KeyboardInterrupt:
        log.info("Interrupted; progress is saved, rerun to resume.")


if __name__ == "__main__":
    main()
