"""Process the English Wikipedia XML dump with Spark: extract articles and redirects with regex.

The dump (enwiki-*-pages-articles*.xml.bz2) is read with spark-xml, one row per
<page>, as in the lecture example (read_xml.py). Bzip2 is splittable, so Spark
reads one dump file in many partitions in parallel. Only the main namespace
(ns = 0, i.e. articles) is kept.

All extraction is done with regular expressions on the wikitext, using Spark SQL
functions (regexp_extract / regexp_replace), which run inside the JVM - no
Python per row, so the full dump is processed in one pass:

  redirects  title -> target, and whether it is an "R from E number" redirect
             (Wikipedia has redirects like E951 -> Aspartame, E322 -> Lecithin)
  articles   short description, infobox type and selected infobox fields
             (owner, parent, founded, country, industry, ...), categories,
             disambiguation flag, the first paragraph (abstract) and a
             health / safety / controversy section if the article has one

Output (Parquet, partitioned by kind=article|redirect): spark_output/wiki_pages

Usage:
  python spark_wiki.py --input wiki_dump/enwiki-latest-pages-articles1.xml-p1p41242.bz2   # one part
  python spark_wiki.py --input wiki_dump/enwiki-latest-pages-articles.xml.bz2              # full dump
"""

from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_common import create_spark, stats_path

DEFAULT_INPUT = "wiki_dump/enwiki-latest-pages-articles.xml.bz2"
DEFAULT_OUTPUT = "spark_output/wiki_pages"

# Only the parts of <page> we need; spark-xml puts element text with attributes in _VALUE.
PAGE_SCHEMA = StructType([
    StructField("title", StringType()),
    StructField("ns", StringType()),
    StructField("redirect", StructType([StructField("_title", StringType())])),
    StructField("revision", StructType([
        StructField("text", StructType([StructField("_VALUE", StringType())])),
    ])),
])

# ---------------------------------------------------------------------------
# Regex patterns (Java regex syntax, used by Spark SQL)
# ---------------------------------------------------------------------------

SHORT_DESCRIPTION_RE = r"(?i)\{\{\s*short description\s*\|\s*([^}\n|]*)"
# {{Infobox company ...}}, {{Infobox brand ...}}, {{chembox ...}}, {{Speciesbox ...}}
INFOBOX_TYPE_RE = r"(?i)\{\{\s*(infobox[ _][^|\n}<]*|chembox|drugbox|speciesbox|taxobox|automatic taxobox)"
DISAMBIGUATION_RE = (r"(?i)\{\{\s*(disambiguation|disambig|dab|hndis|geodis|surname|given name|"
                     r"set index article|disambiguation cleanup)\s*(\||\}\})")
CATEGORY_RE = r"\[\[\s*[Cc]ategory\s*:\s*([^\]|]+)"
# Lead section = everything before the first "== Heading =="
LEAD_RE = r"(?s)^(.*?)(?:\n==|\z)"
# A section whose heading mentions safety / health / toxicity / controversy / criticism
HEALTH_SECTION_RE = (r"(?s)\n(={2,3})\s*([^=\n]*?(?:[Ss]afety|[Hh]ealth|[Tt]oxicity|[Cc]ontrovers|"
                     r"[Ee]nvironmental (?:impact|issues)|[Cc]riticism)[^=\n]*?)\s*\1\s*\n(.*?)(?=\n={2,3}[^=]|\z)")
YEAR_RE = r"\b(1[5-9][0-9]{2}|20[0-9]{2})\b"
E_NUMBER_REDIRECT_RE = r"(?i)\{\{\s*R from E number"

# Infobox fields we keep: output column -> parameter names used in the infoboxes
INFOBOX_FIELDS = {
    "owner": ["currentowner", "owner", "owners"],
    "parent": ["parent"],
    "founded_year": ["founded", "introduced", "foundation", "formation"],
    "founder": ["founder", "founders"],
    "country": ["country", "hq_location_country", "origin"],
    "industry": ["industry"],
    "products": ["products"],
    "capital": ["capital"],
}


def infobox_field(text: Column, names: list) -> Column:
    """Raw value of '| name = value' (first matching parameter, one line).

    Only spaces/tabs are skipped around '=': with \\s an empty parameter
    ('| parent =' + newline) would swallow the line break and return the next line.
    """
    pattern = r"(?im)^[ \t]*\|[ \t]*(?:" + "|".join(names) + r")[ \t]*=[ \t]*([^\n]*)"
    return F.regexp_extract(text, pattern, 1)


def strip_whitespace(text: Column) -> Column:
    """Collapse all whitespace (incl. newlines) to single spaces and trim."""
    return F.regexp_replace(F.regexp_replace(text, r"\s+", " "), r"^ | $", "")


def clean_value(text: Column) -> Column:
    """Light cleaning for one-line infobox values: references, templates, link markup, tags."""
    text = F.regexp_replace(text, r"(?is)<ref[^>]*?(?:/>|>.*?</ref>)", "")
    text = F.regexp_replace(text, r"(?i)<br\s*/?>", ", ")
    text = F.regexp_replace(text, r"\{\{[^{}]*\}\}", "")
    text = F.regexp_replace(text, r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", "$1")
    text = F.regexp_replace(text, r"'{2,}|<[^>]+>|<!--.*", "")
    return strip_whitespace(text)


def clean_markup(text: Column, template_depth: int = 3) -> Column:
    """Wikitext -> plain text with regex: drop comments, references, templates,
    tables and files; keep the visible text of links; drop bold/italic quotes and tags."""
    text = F.regexp_replace(text, r"(?s)<!--.*?-->", "")
    text = F.regexp_replace(text, r"(?is)<ref[^>/]*/>", "")
    text = F.regexp_replace(text, r"(?is)<ref[^>]*>.*?</ref>", "")
    text = F.regexp_replace(text, r"(?i)<br\s*/?>", ", ")
    for _ in range(template_depth):  # templates nest: remove the innermost {{...}} level each pass
        text = F.regexp_replace(text, r"\{\{[^{}]*\}\}", "")
    text = F.regexp_replace(text, r"(?s)\{\|.*?\|\}", "")
    text = F.regexp_replace(text, r"(?i)\[\[(?:File|Image):[^\[\]]*(?:\[\[[^\]]*\]\][^\[\]]*)*\]\]", "")
    text = F.regexp_replace(text, r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", "$1")
    text = F.regexp_replace(text, r"\[https?://\S+\s*([^\]]*)\]", "$1")
    text = F.regexp_replace(text, r"'{2,}", "")
    # Removed pronunciation templates leave "Ferrero SpA ( , )" / "Kinder (; German ...)".
    text = F.regexp_replace(text, r"\s*\(\s*[,;:]?\s*[,;:]?\s*\)", "")
    text = F.regexp_replace(text, r"\(\s*[,;:]+\s*", "(")
    text = F.regexp_replace(text, r"<[^>]+>", "")
    text = F.regexp_replace(text, r"&nbsp;", " ")
    return F.regexp_replace(F.regexp_replace(text, r"[ \t]+", " "), r"(?m)^ +| +$", "")


def blank_to_null(column: Column) -> Column:
    return F.when(F.length(column) > 0, column)


def build(pages):
    """Main-namespace pages -> one table of articles and redirects with extracted fields."""
    text = F.col("revision.text._VALUE")
    is_redirect = F.col("redirect._title").isNotNull()
    article = ~is_redirect

    def for_articles(column: Column) -> Column:
        return F.when(article, column)

    # Infoboxes nest templates deeply, so the lead gets 6 removal passes; lines that
    # still start like infobox/table markup (| { } !) are not taken as the abstract.
    lead = clean_markup(F.regexp_extract(text, LEAD_RE, 1), template_depth=6)
    columns = {
        "kind": F.when(is_redirect, F.lit("redirect")).otherwise(F.lit("article")),
        "title": F.col("title"),
        # redirects
        "target": F.regexp_replace(F.col("redirect._title"), r"#.*$", ""),
        "from_e_number": F.when(is_redirect, text.rlike(E_NUMBER_REDIRECT_RE)),
        # articles
        "short_description": for_articles(blank_to_null(F.trim(F.regexp_extract(text, SHORT_DESCRIPTION_RE, 1)))),
        "infobox_type": for_articles(blank_to_null(F.trim(F.regexp_replace(
            F.lower(F.regexp_extract(text, INFOBOX_TYPE_RE, 1)), r"^infobox[ _]+", "")))),
        "is_disambiguation": for_articles(text.rlike(DISAMBIGUATION_RE)),
        "categories": for_articles(F.regexp_extract_all(text, F.lit(CATEGORY_RE), 1)),
        "abstract": for_articles(blank_to_null(F.substring(
            F.regexp_extract(lead, r"(?m)^([^|{}!*#:\n<][^\n]{39,})", 1), 1, 1000))),
        "health_section_title": for_articles(blank_to_null(F.trim(F.regexp_extract(text, HEALTH_SECTION_RE, 2)))),
        "health_section": for_articles(blank_to_null(F.substring(
            strip_whitespace(clean_markup(F.regexp_extract(text, HEALTH_SECTION_RE, 3))), 1, 1500))),
        "text_length": for_articles(F.length(text)),
    }
    for name, params in INFOBOX_FIELDS.items():
        raw = infobox_field(text, params)
        if name == "founded_year":  # dates are templates ({{start date|1976|...}}): keep the year
            columns[name] = for_articles(blank_to_null(F.regexp_extract(raw, YEAR_RE, 1)))
        else:
            columns[name] = for_articles(blank_to_null(F.substring(clean_value(raw), 1, 300)))
    return pages.select([column.alias(name) for name, column in columns.items()])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    started = time.time()
    # Dozens of nested regex expressions exceed the 64 KB limit of Spark's whole-stage
    # generated code; per-expression code generation avoids the fallback warnings.
    spark = create_spark("wiki-dump", xml=True, conf={"spark.sql.codegen.wholeStage": "false"})
    pages = (spark.read.format("com.databricks.spark.xml")
             .option("rowTag", "page")
             .schema(PAGE_SCHEMA)
             .load(args.input)
             .filter(F.col("ns") == "0"))

    build(pages).write.mode("overwrite").partitionBy("kind").parquet(args.output)

    out = spark.read.parquet(args.output)
    articles = out.filter("kind = 'article'")
    stats = {
        "input": args.input,
        "seconds": round(time.time() - started),
        "articles": articles.count(),
        "redirects": out.filter("kind = 'redirect'").count(),
        "e_number_redirects": out.filter("from_e_number").count(),
        "disambiguation_pages": articles.filter("is_disambiguation").count(),
        "articles_with_infobox": articles.filter("infobox_type IS NOT NULL").count(),
        "articles_with_health_section": articles.filter("health_section IS NOT NULL").count(),
        "top_infobox_types": {r["infobox_type"]: r["count"] for r in
                              articles.groupBy("infobox_type").count().filter("infobox_type IS NOT NULL")
                              .orderBy(F.desc("count")).limit(15).collect()},
    }
    with open(stats_path(args.output), "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=1)
    print(json.dumps(stats, indent=1))
    spark.stop()


if __name__ == "__main__":
    main()
