"""Own inverted index over the extracted products (no database, no Lucene).

Reads products_data.jsonl (written by extractor.py) and writes index/index.json:

  terms      term -> postings "doc:tf,doc:tf,..." (doc = row number of the product)
  docs       per document: the fields shown in search results and used by filters
  norms      per idf method: the length of every document vector, for cosine similarity

Each relevant product is one document. A product is relevant when it has a
name and at least ingredients or nutrition facts; the others (~4 %) are nearly
empty pages that cannot be shown as a useful result and, being very short,
would otherwise rank high under cosine similarity. Its text is the product name, brands, categories,
labels, countries, ingredients, additives (codes and names), allergens
and traces. Name and brands
are counted twice (a simple field boost), so a product named "Nutella" ranks
above one that only mentions Nutella in its ingredients.

The index is rebuilt from scratch on every run (about a minute for 80k
products), after the extractor has been re-run.

Usage:
  python indexer.py
"""

from __future__ import annotations

import json
import math
import os
import re
import time
import unicodedata
from collections import Counter

INPUT_FILE = "products_data.jsonl"
INDEX_DIR = "index"
INDEX_FILE = os.path.join(INDEX_DIR, "index.json")

# Text fields of a product and how many times each is counted (field boost).
FIELD_WEIGHTS = {
    "product_name": 2,
    "brands": 2,
    "categories": 1,
    "labels": 1,
    "countries": 1,
    "ingredients_text": 1,
    "additives": 1,
    "additive_names": 1,
    "allergens": 1,
    "traces": 1,
}

# Most products are in English or French, so stop words of both are removed.
STOP_WORDS = set("""
a an and are as at be by for from has have in is it its of on or that the this to was
with without per contains may contain
au aux avec ce ces dans de des du en et la le les leur ou par pour sans sur un une
y d l en
""".split())

TOKEN_RE = re.compile(r"[a-z0-9]+")


# ---------------------------------------------------------------------------
# Text operations (shared with search.py)
# ---------------------------------------------------------------------------

def fold_accents(text: str) -> str:
    """'Crème brûlée' -> 'Creme brulee', so queries work without diacritics."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def tokenize(text: str) -> list:
    """Lowercase, remove accents, split on non-alphanumerics, drop stop words,
    one-letter tokens and plain numbers. No lemmatization or stemming."""
    tokens = TOKEN_RE.findall(fold_accents(text.lower()))
    return [t for t in tokens if len(t) > 1 and not t.isdigit() and t not in STOP_WORDS]


def is_relevant(product: dict) -> bool:
    return bool(product["product_name"] and (product["ingredients_text"] or product["nutrition_100g"]))


def document_text(product: dict) -> str:
    parts = []
    for field, weight in FIELD_WEIGHTS.items():
        value = product.get(field)
        if not value:
            continue
        text = " ".join(value) if isinstance(value, list) else str(value)
        parts.extend([text] * weight)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# IDF methods (shared with search.py)
# ---------------------------------------------------------------------------
# Three variants from the standard tf-idf weighting schemes. The probabilistic
# idf log((N - df) / df) is deliberately not used (not allowed in the project).

def idf_classic(n_docs: int, df: int, max_df: int) -> float:
    """log(N / df): 0 for a term that is in every document."""
    return math.log10(n_docs / df)


def idf_smooth(n_docs: int, df: int, max_df: int) -> float:
    """log(N / (1 + df)) + 1: never 0, so very common terms still count a little."""
    return math.log10(n_docs / (1 + df)) + 1


def idf_max(n_docs: int, df: int, max_df: int) -> float:
    """log(max_df / (1 + df)): relative to the most frequent term in the collection
    instead of the collection size."""
    return max(math.log10(max_df / (1 + df)), 0.0)


IDF_METHODS = {"classic": idf_classic, "smooth": idf_smooth, "max": idf_max}


def tf_weight(tf: int) -> float:
    """Sublinear tf: 1 + log(tf), so 10 occurrences are not 10x as important as one."""
    return 1 + math.log10(tf)


# ---------------------------------------------------------------------------
# Building the index
# ---------------------------------------------------------------------------

def build_index(input_file: str = INPUT_FILE) -> dict:
    postings = {}  # term -> list of (doc_id, tf)
    docs = []
    skipped = 0
    with open(input_file, encoding="utf-8") as f:
        for line in f:
            product = json.loads(line)
            if not is_relevant(product):
                skipped += 1
                continue
            doc_id = len(docs)
            for term, tf in Counter(tokenize(document_text(product))).items():
                postings.setdefault(term, []).append((doc_id, tf))
            docs.append({
                "barcode": product["barcode"],
                "name": product["product_name"],
                "brands": product["brands"],
                "url": product["url"],
                "nutri_score": product["nutri_score"],
                "eco_score": product["eco_score"],
                "nutrition_100g": product["nutrition_100g"],
            })

    n_docs = len(docs)
    max_df = max(len(p) for p in postings.values())

    # Document vector lengths for each idf method (needed for cosine similarity).
    norms = {}
    for method, idf in IDF_METHODS.items():
        squares = [0.0] * n_docs
        for term_postings in postings.values():
            weight = idf(n_docs, len(term_postings), max_df)
            for doc_id, tf in term_postings:
                squares[doc_id] += (tf_weight(tf) * weight) ** 2
        norms[method] = [round(math.sqrt(s), 6) for s in squares]

    return {
        "n_docs": n_docs,
        "skipped": skipped,
        "max_df": max_df,
        "terms": {t: ",".join(f"{d}:{tf}" for d, tf in p) for t, p in postings.items()},
        "docs": docs,
        "norms": norms,
    }


def main() -> None:
    started = time.time()
    index = build_index()
    os.makedirs(INDEX_DIR, exist_ok=True)
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)
    n_postings = sum(p.count(",") + 1 for p in index["terms"].values())
    print(f"Indexed {index['n_docs']} documents ({index['skipped']} not relevant, skipped), "
          f"{len(index['terms'])} terms, "
          f"{n_postings} postings in {time.time() - started:.0f} s")
    print(f"Index file: {INDEX_FILE} ({os.path.getsize(INDEX_FILE) / 1024 / 1024:.1f} MB)")


if __name__ == "__main__":
    main()
