#!/bin/sh
# Download the English Wikipedia dump (part 1 for development first, then the full dump).
# -C - resumes an interrupted download.
cd "$(dirname "$0")"
UA="VINF-university-project-wiki-download/1.0"
BASE=https://dumps.wikimedia.org/enwiki/latest
curl -sS -A "$UA" -C - -o enwiki-latest-pages-articles1.xml-p1p41242.bz2 "$BASE/enwiki-latest-pages-articles1.xml-p1p41242.bz2" && echo "part1 done $(date)"
curl -sS -A "$UA" -C - -o enwiki-latest-pages-articles.xml.bz2 "$BASE/enwiki-latest-pages-articles.xml.bz2" && echo "full dump done $(date)"
