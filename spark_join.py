"""Join the crawled products with Wikipedia articles (Spark DataFrames).

Inputs:  spark_output/products     (spark_extract_products.py)
         spark_output/wiki_pages   (spark_wiki.py)

Each product gives join values of four types:
  brand       brands               "Milka"         -> Milka (Infobox brand: owner Mondelez)
  additive    E-numbers, names     "E951"          -> Aspartame (via the redirect E951)
  ingredient  English ingredients  "palm oil"      -> Palm oil
  country     countries where sold "France"        -> France

Join logic:
  1. Every value is normalised to a key (lowercase, no accents, alphanumerics only);
     Wikipedia titles and redirect titles are normalised the same way.
  2. key -> page through article titles and redirects (E951 -> Aspartame).
     Brands also try "<brand> (brand)" and "<brand> (company)", because e.g.
     "Ferrero" is a disambiguation page and Kinder's article is "Kinder (brand)".
  3. A match is accepted only if the article fits the join type (brand -> company or
     brand article, additive -> E-number redirect or chemical article, ...);
     disambiguation pages never count.
  4. Per product value the best candidate is kept (exact name first, direct article
     before redirect).

Outputs (spark_output/joined/):
  links/              barcode, product, join type, value -> Wikipedia title (inner join)
  wiki_attributes/    the joined Wikipedia pages and their extracted attributes
  products_enriched/  all products + their Wikipedia data (left join: products
                      without a match are kept with empty lists)
  stats.json          matched products / values / unique Wikipedia pages per type

Usage:
  python spark_join.py
  python spark_join.py --wiki spark_output/wiki_dev     # development run on one dump part
"""

from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_common import create_spark, normalize_key, stats_path

# Which Wikipedia articles are valid targets for each join type.
VALID_TARGET = {
    "brand": (
        # A brand must match a company or brand article - not a generic food article
        # such as "Cappuccino" or "Cola", whose descriptions also mention food/drink.
        "infobox_type rlike '^(company|brand|retail|supermarket|organization|cooperative"
        "|restaurant chain|dot-com company)' "
        "OR short_description rlike '(?i)\\\\b(brand|company|manufacturer|corporation|supermarket"
        "|retailer|chain|brewery|cooperative|conglomerate)\\\\b' "
        "OR exists(categories, c -> c rlike '(?i)\\\\bbrands\\\\b|companies|manufacturers|breweries')"
    ),
    "brand_not_food": (
        # Same name, different business: e.g. the store brand "U" must not become a TV
        # company. Companies whose description/industry is clearly not food are excluded.
        "concat_ws(' ', short_description, industry, infobox_type) rlike '(?i)television|broadcast"
        "|tv channel|airline|airways|aviation|\\\\bbank|insurance|software|internet|telecom|mobile"
        "|electricity|energy|petroleum|oil and gas|hotel|registry|camera|optic|clothing|fashion"
        "|apparel|footwear|bicycle|cycling|automotive|video game|record label|publish|newspaper"
        "|magazine|university|political|charity|sports club|football|real estate|consulting"
        "|semiconductor|electronics'"
    ),
    "additive": (
        "from_e_number "
        "OR exists(categories, c -> c rlike '(?i)\\\\b(E-number additives|food additives|food colorings"
        "|sweeteners|preservatives|emulsifiers|antioxidants|acidity regulators|thickening agents)\\\\b') "
        "OR infobox_type in ('chembox', 'drugbox', 'drug', 'chemical compound')"
    ),
    "ingredient": (
        # Category words must be whole words ("herbs" must not match "Suburbs"); the bare
        # word "nuts" is left out ("NUTS 3 statistical regions"), "Edible nuts" is covered
        # by "edible". Places, people and organisations are never ingredients.
        "NOT coalesce(infobox_type rlike '^(settlement|country|person|officeholder|company|organization"
        "|football club|sports team|military unit|film|album|book|musical artist|university|school)', false) "
        "AND (infobox_type rlike '(?i)food|chembox|drugbox|^drug|speciesbox|taxobox|nutritional value"
        "|beverage|drink|cheese|cultivar|plant|spice|grape variety' "
        "OR exists(categories, c -> c rlike '(?i)\\\\b(foods?|ingredients?|edible|oils|spices|herbs"
        "|vegetables|fruits|sweeteners|additives|dairy|cereals|flours|starches|sugars"
        "|condiments|meat|seafood|crops|legumes)\\\\b'))"
    ),
    "country": "infobox_type rlike '^(country|sovereign state)' OR short_description rlike '(?i)^country'",
}

JOIN_TYPES = ["brand", "additive", "ingredient", "country"]

# Attributes of the joined Wikipedia pages that are kept for the search index.
WIKI_ATTRIBUTES = ["title", "short_description", "infobox_type", "abstract", "owner", "parent",
                   "founded_year", "founder", "country", "industry", "products", "capital",
                   "health_section_title", "health_section", "categories"]


def product_values(products: DataFrame) -> DataFrame:
    """(barcode, join_type, value, candidate, priority) - one row per candidate title."""
    brands = (products.select("barcode", F.explode("brands").alias("value"))
              .withColumn("candidates", F.array(
                  F.struct(F.col("value").alias("candidate"), F.lit(0).alias("priority")),
                  F.struct(F.concat(F.col("value"), F.lit(" (brand)")).alias("candidate"), F.lit(1).alias("priority")),
                  F.struct(F.concat(F.col("value"), F.lit(" (company)")).alias("candidate"), F.lit(2).alias("priority")),
              ))
              .select("barcode", F.lit("brand").alias("join_type"), "value", F.explode("candidates").alias("c"))
              .select("barcode", "join_type", "value", "c.candidate", "c.priority"))

    additive_codes = products.select("barcode", F.lit("additive").alias("join_type"),
                                     F.explode("additives").alias("value"),
                                     F.lit(0).alias("priority"))
    additive_names = products.select("barcode", F.lit("additive").alias("join_type"),
                                     F.explode("additive_names").alias("value"),
                                     F.lit(1).alias("priority"))
    additives = (additive_codes.unionByName(additive_names)
                 .withColumn("candidate", F.col("value"))
                 .select("barcode", "join_type", "value", "candidate", "priority"))

    # Ingredient phrases: split the English ingredient list on punctuation, brackets,
    # " and " / " & "; drop percentages and numbers; keep phrases of 1-3 words.
    ingredient_text = F.regexp_replace(F.lower("ingredients_text"), r"[()\[\]{}]", ",")
    ingredients = (products.filter(F.col("ingredients_language") == "English")
                   .select("barcode", F.explode(F.split(ingredient_text, r"[,;:.*]| and | & ")).alias("raw"))
                   .withColumn("value", F.trim(F.regexp_replace("raw", r"[0-9]+(?:[.,][0-9]+)?\s*%?|%", "")))
                   .filter((F.length("value").between(3, 40)) & (F.size(F.split("value", r"\s+")) <= 3))
                   .select("barcode", F.lit("ingredient").alias("join_type"), "value",
                           F.col("value").alias("candidate"), F.lit(0).alias("priority")))

    countries = products.select("barcode", F.lit("country").alias("join_type"),
                                F.explode("countries").alias("value"),
                                F.lit(0).alias("priority")).withColumn("candidate", F.col("value"))
    countries = countries.select("barcode", "join_type", "value", "candidate", "priority")

    return (brands.unionByName(additives).unionByName(ingredients).unionByName(countries)
            .withColumn("key", normalize_key(F.col("candidate")))
            # Keys under 3 characters ("u", "h") are too ambiguous to match a title.
            .filter(F.length("key") >= 3)
            .dropDuplicates(["barcode", "join_type", "value", "key"]))


def title_map(wiki: DataFrame) -> DataFrame:
    """key -> page: article titles map to themselves, redirect titles to their target."""
    articles = (wiki.filter("kind = 'article'")
                .select(normalize_key(F.col("title")).alias("key"), F.col("title").alias("page"),
                        F.lit(False).alias("via_redirect"), F.lit(False).alias("from_e_number")))
    redirects = (wiki.filter("kind = 'redirect'")
                 .select(normalize_key(F.col("title")).alias("key"), F.col("target").alias("page"),
                         F.lit(True).alias("via_redirect"), F.coalesce("from_e_number", F.lit(False)).alias("from_e_number")))
    return articles.unionByName(redirects)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--products", default="spark_output/products")
    parser.add_argument("--wiki", default="spark_output/wiki_pages")
    parser.add_argument("--output", default="spark_output/joined")
    args = parser.parse_args()

    started = time.time()
    spark = create_spark("join-products-wiki")
    products = spark.read.parquet(args.products)
    wiki = spark.read.parquet(args.wiki)
    articles = wiki.filter("kind = 'article'").drop("kind", "target", "from_e_number")

    values = product_values(products)
    candidates = (values.join(title_map(wiki), "key")
                  .join(articles, F.col("page") == articles.title)
                  .filter(~F.col("is_disambiguation")))
    # Generic pages ("Brand", "E number", "List of ...") are never a valid target.
    candidates = candidates.filter(~F.col("title").rlike(r"^(List of |Lists of )|^(Brand|E number|Company|Food additive)$"))
    valid = None
    for join_type in JOIN_TYPES:
        condition = F.expr(VALID_TARGET[join_type])
        if join_type == "brand":
            condition = condition & ~F.coalesce(F.expr(VALID_TARGET["brand_not_food"]), F.lit(False))
        part = candidates.filter((F.col("join_type") == join_type) & condition)
        valid = part if valid is None else valid.unionByName(part)

    # Best candidate per product value: lowest priority, then a direct article before a redirect.
    best = Window.partitionBy("barcode", "join_type", "value").orderBy("priority", "via_redirect")
    links = (valid.withColumn("rank", F.row_number().over(best)).filter("rank = 1")
             .select("barcode", "join_type", "value", F.col("candidate").alias("matched_on"),
                     "via_redirect", F.col("page").alias("wiki_title"))
             .dropDuplicates(["barcode", "join_type", "wiki_title"]))
    links = links.join(products.select("barcode", "product_name"), "barcode").cache()

    links.write.mode("overwrite").parquet(f"{args.output}/links")
    links.drop("via_redirect").write.mode("overwrite").option("header", True).csv(f"{args.output}/links_csv")

    joined_titles = links.select(F.col("wiki_title").alias("title")).distinct()
    wiki_attributes = articles.join(joined_titles, "title").select(WIKI_ATTRIBUTES)
    wiki_attributes.write.mode("overwrite").parquet(f"{args.output}/wiki_attributes")

    # Left join: every product, with its Wikipedia data grouped by join type.
    linked = (links.join(wiki_attributes, links.wiki_title == wiki_attributes.title)
              .groupBy("barcode")
              .agg(*[F.collect_list(F.when(F.col("join_type") == t, F.struct(
                  F.col("value"), *[F.col(a) for a in WIKI_ATTRIBUTES if a != "categories"])))
                  .alias(f"wiki_{t}s") for t in JOIN_TYPES]))
    enriched = products.join(linked, "barcode", how="left")
    enriched.write.mode("overwrite").json(f"{args.output}/products_enriched")

    # Statistics
    stats = {"seconds": None, "products": products.count(),
             "products_with_any_match": links.select("barcode").distinct().count(),
             "unique_wiki_pages": joined_titles.count(), "by_type": {}}
    for join_type in JOIN_TYPES:
        typed_values = values.filter(F.col("join_type") == join_type)
        typed_links = links.filter(F.col("join_type") == join_type)
        stats["by_type"][join_type] = {
            "products_with_values": typed_values.select("barcode").distinct().count(),
            "products_matched": typed_links.select("barcode").distinct().count(),
            "distinct_values": typed_values.select("value").distinct().count(),
            "distinct_values_matched": typed_links.select("value").distinct().count(),
            "unique_wiki_pages": typed_links.select("wiki_title").distinct().count(),
            "links": typed_links.count(),
            "top_pages": {r["wiki_title"]: r["count"] for r in typed_links.groupBy("wiki_title").count()
                          .orderBy(F.desc("count")).limit(10).collect()},
        }
    stats["seconds"] = round(time.time() - started)
    with open(stats_path(args.output), "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=1, ensure_ascii=False)
    print(json.dumps(stats, indent=1, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
