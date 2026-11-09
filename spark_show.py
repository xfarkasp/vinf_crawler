"""Show the results of the Spark jobs in a readable form (for checking and demos).

Usage:
  python spark_show.py product 3017620422003      # a product and its Wikipedia links
  python spark_show.py wiki "Aspartame"           # what was extracted from one article
  python spark_show.py redirect E951              # where a redirect leads
  python spark_show.py value brand Milka          # which products a join value links
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

from spark_common import create_spark

OUT = "spark_output"


def show_product(spark, barcode: str) -> None:
    products = spark.read.parquet(f"{OUT}/products")
    p = products.filter(F.col("barcode") == barcode).first()
    if not p:
        print(f"No product with barcode {barcode}")
        return
    print(f"{p.product_name}  ({p.barcode})")
    print(f"  brands: {p.brands}  additives: {p.additives}  countries: {p.countries}")
    links = spark.read.parquet(f"{OUT}/joined/links").filter(F.col("barcode") == barcode)
    attrs = spark.read.parquet(f"{OUT}/joined/wiki_attributes")
    rows = links.join(attrs, links.wiki_title == attrs.title).orderBy("join_type", "value").collect()
    print(f"  {len(rows)} Wikipedia links:")
    for r in rows:
        print(f"   {r.join_type:<10} {r.value!r:<28} -> {r.wiki_title}")
        detail = r.short_description or (r.abstract or "")[:90]
        extra = ", ".join(f"{k}: {getattr(r, k)}" for k in ("owner", "founded_year", "country") if getattr(r, k))
        print(f"   {'':<10} {'':<28}    {detail}{' | ' + extra if extra else ''}")


def show_wiki(spark, title: str) -> None:
    pages = spark.read.parquet(f"{OUT}/wiki_pages")
    r = pages.filter((F.col("title") == title) & (F.col("kind") == "article")).first()
    if not r:
        print(f"No article titled {title!r}")
        return
    for field in ("title", "short_description", "infobox_type", "is_disambiguation", "owner", "parent",
                  "founded_year", "founder", "country", "industry", "capital"):
        print(f"  {field:<18} {getattr(r, field)}")
    print(f"  {'abstract':<18} {(r.abstract or '')[:300]}")
    print(f"  {'health_section':<18} [{r.health_section_title}] {(r.health_section or '')[:200]}")
    print(f"  {'categories':<18} {(r.categories or [])[:6]}")


def show_redirect(spark, title: str) -> None:
    pages = spark.read.parquet(f"{OUT}/wiki_pages")
    r = pages.filter((F.col("title") == title) & (F.col("kind") == "redirect")).first()
    print(f"  {title} -> {r.target}  (R from E number: {r.from_e_number})" if r else f"No redirect titled {title!r}")


def show_value(spark, join_type: str, value: str) -> None:
    links = spark.read.parquet(f"{OUT}/joined/links")
    rows = links.filter((F.col("join_type") == join_type) & (F.lower("value") == value.lower()))
    print(f"  {join_type} {value!r} -> {[r.wiki_title for r in rows.select('wiki_title').distinct().collect()]}")
    print(f"  linked products: {rows.select('barcode').distinct().count()}, e.g.:")
    for r in rows.limit(5).collect():
        print(f"   {r.barcode}  {r.product_name}")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        return
    spark = create_spark("show")
    command, args = sys.argv[1], sys.argv[2:]
    {"product": lambda: show_product(spark, args[0]),
     "wiki": lambda: show_wiki(spark, " ".join(args)),
     "redirect": lambda: show_redirect(spark, " ".join(args)),
     "value": lambda: show_value(spark, args[0], " ".join(args[1:]))}.get(command, lambda: print(__doc__))()
    spark.stop()


if __name__ == "__main__":
    main()
