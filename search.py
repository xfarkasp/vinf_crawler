"""Own search engine over the index built by indexer.py.

Ranking: TF-IDF vectors with cosine similarity,
  weight(term, doc)   = (1 + log tf) * idf(term)
  weight(term, query) = (1 + log tf) * idf(term)
  cosine(query, doc)  = sum of query weight * doc weight / (|query| * |doc|)
  score(query, doc)   = cosine * (query terms found in doc / query terms)
with one of three idf methods: classic, smooth, max (see indexer.py).
The last factor (coordination, as in Lucene's classic scoring) ranks products
that match all query terms above short products that match only one.

Query syntax (anything else is a search term):
  +e951          the product must contain the term "e951" (it is also scored)
  -palm          exclude products containing the term "palm"
  -palm*         prefix wildcard: exclude every term starting with "palm"
                 (palme, palmiste, palmol, palmfett ... - the data is multilingual)
  nutri:a        Nutri-Score filter (nutri:a,b allows several grades)
  eco:a          Green-Score / Eco-Score filter (eco:a+,a)
  sugars<5       nutrition filters per 100 g: kcal, fat, carbohydrates, sugars,
  proteins>20    proteins with < or >

Usage:
  python search.py "chocolate hazelnut spread"
  python search.py "breakfast cereals nutri:a -palm" --idf smooth --top 5
  python search.py "chocolate hazelnut spread" --compare      # all idf methods side by side
  python search.py                                            # interactive
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from collections import Counter
from typing import Optional

from indexer import IDF_METHODS, INDEX_FILE, tf_weight, tokenize

NUTRIENT_KEYS = {"kcal": "energy-kcal", "energy": "energy-kcal", "fat": "fat",
                 "carbohydrates": "carbohydrates", "carbs": "carbohydrates",
                 "sugars": "sugars", "sugar": "sugars", "proteins": "proteins", "protein": "proteins"}
FILTER_RE = re.compile(r"^(nutri|eco):([a-f+,]+)$", re.IGNORECASE)
NUTRIENT_FILTER_RE = re.compile(r"^([a-z]+)([<>])(\d+(?:[.,]\d+)?)$", re.IGNORECASE)


class Query:
    """A parsed query: search terms, excluded terms and filters."""

    def __init__(self, text: str):
        self.text = text
        self.terms = []
        self.required = set()
        self.excluded = set()
        self.excluded_prefixes = set()
        self.grades = {}       # "nutri_score" / "eco_score" -> allowed grades
        self.nutrients = []    # (nutrition key, "<" or ">", value)
        for word in text.split():
            grade = FILTER_RE.match(word)
            nutrient = NUTRIENT_FILTER_RE.match(word)
            if grade:
                field = "nutri_score" if grade.group(1).lower() == "nutri" else "eco_score"
                self.grades[field] = {g.upper() for g in grade.group(2).split(",") if g}
            elif nutrient and nutrient.group(1).lower() in NUTRIENT_KEYS:
                key = NUTRIENT_KEYS[nutrient.group(1).lower()]
                self.nutrients.append((key, nutrient.group(2), float(nutrient.group(3).replace(",", "."))))
            elif word.startswith("-") and word.endswith("*") and len(word) > 2:
                self.excluded_prefixes.update(tokenize(word[1:-1]))
            elif word.startswith("-"):
                self.excluded.update(tokenize(word[1:]))
            elif word.startswith("+"):
                self.required.update(tokenize(word[1:]))
                self.terms.extend(tokenize(word[1:]))
            else:
                self.terms.extend(tokenize(word))

    def accepts(self, doc: dict) -> bool:
        for field, allowed in self.grades.items():
            if doc[field] not in allowed:
                return False
        for key, op, value in self.nutrients:
            actual = (doc["nutrition_100g"] or {}).get(key)
            if actual is None or (actual >= value if op == "<" else actual <= value):
                return False
        return True


class SearchEngine:
    def __init__(self, index_file: str = INDEX_FILE):
        with open(index_file, encoding="utf-8") as f:
            index = json.load(f)
        self.n_docs = index["n_docs"]
        self.max_df = index["max_df"]
        self.terms = index["terms"]
        self.docs = index["docs"]
        self.norms = index["norms"]
        self._parsed = {}

    def postings(self, term: str) -> dict:
        """doc id -> tf for a term (parsed from the compact string on first use)."""
        if term not in self._parsed:
            raw = self.terms.get(term)
            self._parsed[term] = (
                {int(d): int(tf) for d, tf in (p.split(":") for p in raw.split(","))} if raw else {}
            )
        return self._parsed[term]

    def idf(self, term: str, method: str) -> float:
        df = len(self.postings(term))
        return IDF_METHODS[method](self.n_docs, df, self.max_df) if df else 0.0

    def search(self, query: Query, method: str = "classic", top: int = 10) -> list:
        """Return [(score, doc), ...] best first."""
        query_weights = {
            term: tf_weight(tf) * self.idf(term, method)
            for term, tf in Counter(query.terms).items()
        }
        query_norm = math.sqrt(sum(w * w for w in query_weights.values())) or 1.0

        excluded_terms = set(query.excluded)
        for prefix in query.excluded_prefixes:
            excluded_terms.update(t for t in self.terms if t.startswith(prefix))
        excluded_docs = set()
        for term in excluded_terms:
            excluded_docs.update(self.postings(term))
        required_postings = [self.postings(term) for term in query.required]

        scores = Counter()
        matched = Counter()  # doc id -> number of query terms it contains
        for term, q_weight in query_weights.items():
            idf = self.idf(term, method)
            for doc_id, tf in self.postings(term).items():
                scores[doc_id] += q_weight * tf_weight(tf) * idf
                matched[doc_id] += 1

        doc_norms = self.norms[method]
        results = []
        for doc_id, score in scores.items():
            doc = self.docs[doc_id]
            if (doc_id in excluded_docs or not query.accepts(doc) or not doc_norms[doc_id]
                    or any(doc_id not in postings for postings in required_postings)):
                continue
            coordination = matched[doc_id] / len(query_weights)
            results.append((score / (query_norm * doc_norms[doc_id]) * coordination, doc))
        results.sort(key=lambda r: -r[0])
        return results[:top]


def describe(doc: dict) -> str:
    brands = ", ".join(doc["brands"] or []) or "?"
    nutrition = doc["nutrition_100g"] or {}
    sugars = nutrition.get("sugars")
    return (f"{doc['name'] or '(no name)'} — {brands} | Nutri-Score {doc['nutri_score'] or '?'}"
            f" | Green-Score {doc['eco_score'] or '?'}"
            f" | sugars {f'{sugars:g}' if sugars is not None else '?'} g | {doc['barcode']}")


def print_results(engine: SearchEngine, text: str, method: str, top: int) -> None:
    started = time.time()
    results = engine.search(Query(text), method, top)
    print(f'\n"{text}"  [idf: {method}]  {len(results)} shown, {time.time() - started:.2f} s')
    for rank, (score, doc) in enumerate(results, 1):
        print(f"{rank:>3}. {score:.3f}  {describe(doc)}")


def print_comparison(engine: SearchEngine, text: str, top: int) -> None:
    """Show which products each idf method puts in the top results."""
    query = Query(text)
    print(f'\n"{text}"  — idf of each query term:')
    for term in dict.fromkeys(query.terms):
        df = len(engine.postings(term))
        idfs = "  ".join(f"{m}={engine.idf(term, m):.3f}" for m in IDF_METHODS)
        print(f"   {term:<15} df={df:<6} {idfs}")
    for method in IDF_METHODS:
        print_results(engine, text, method, top)


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(description="Search the OpenFoodFacts product index")
    parser.add_argument("query", nargs="?", help="query; omit for interactive mode")
    parser.add_argument("--idf", choices=list(IDF_METHODS), default="classic")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--compare", action="store_true", help="show results of every idf method")
    args = parser.parse_args(argv)

    started = time.time()
    engine = SearchEngine()
    print(f"Loaded index: {engine.n_docs} documents, {len(engine.terms)} terms ({time.time() - started:.1f} s)")

    queries = [args.query] if args.query else iter(lambda: input("\nquery> ").strip(), "")
    if not args.query:
        print("Interactive mode: empty line to quit.")
    for text in queries:
        if args.compare:
            print_comparison(engine, text, args.top)
        else:
            print_results(engine, text, args.idf, args.top)


if __name__ == "__main__":
    main()
