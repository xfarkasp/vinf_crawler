# Consultation 3 — Big data processing with Spark

Code: <https://github.com/xfarkasp/vinf_crawler> · Spark: **Apache Spark 3.5.3** (PySpark) + `spark-xml` 0.18.0, run locally on all cores (Apple M5, 10 cores, 24 GB RAM); the scripts take HDFS paths and a `SPARK_MASTER` for a cluster.

## 1. Moving the code from the first submission to Spark

| Script | What it does |
|---|---|
| [spark_extract_products.py](https://github.com/xfarkasp/vinf_crawler/blob/main/spark_extract_products.py) | Product extraction from the crawled HTML, distributed: the 99,328 stored pages are split into partitions and every Spark worker runs **the same regex extractor** as in the first submission (`extractor.extract_product`, unit-tested) |
| [spark_wiki.py](https://github.com/xfarkasp/vinf_crawler/blob/main/spark_wiki.py) | Processing of the English Wikipedia dump with regular expressions |
| [spark_join.py](https://github.com/xfarkasp/vinf_crawler/blob/main/spark_join.py) | Join of the products with Wikipedia (Spark DataFrames) |
| [spark_common.py](https://github.com/xfarkasp/vinf_crawler/blob/main/spark_common.py) | Spark session and the join-key normalisation shared by both sides |

**The "small files problem".** The first Spark version read the pages with `sc.binaryFiles` (Hadoop file layer). Reading 99k small files that way has a large per-file overhead — it was slower than the sequential extractor. For a local disk the driver now only lists the paths and the workers open the files directly; on HDFS the `--reader binary` option keeps `binaryFiles`.

| Extraction of 99,328 products | Time |
|---|---|
| Sequential `extractor.py` (1 core) | ~160 s |
| Spark, `sc.binaryFiles` | 365 s |
| Spark, workers read the files directly (10 cores) | **34 s** |

The Spark output is identical to the sequential one (all 99,328 products, every field compared).

## 2. Processing the Wikipedia dump

**Data:** `enwiki-latest-pages-articles.xml.bz2` from [dumps.wikimedia.org/enwiki/latest](https://dumps.wikimedia.org/enwiki/latest/) (dump of 7.11.2026), **25.9 GB** compressed, ~110 GB of XML. Download script: [wiki_dump/download.sh](https://github.com/xfarkasp/vinf_crawler/blob/main/wiki_dump/download.sh).

The dump is read with `spark-xml`, one row per `<page>` — as in the lecture example `read_xml.py`. Bzip2 is splittable, so the single dump file is processed in 772 partitions in parallel. Only the article namespace (`ns = 0`) is kept.

```python
pages = (spark.read.format("com.databricks.spark.xml")
         .option("rowTag", "page")
         .schema(PAGE_SCHEMA)           # title, ns, redirect/@title, revision/text
         .load(args.input)
         .filter(F.col("ns") == "0"))
```

All extraction is done with **regular expressions in Spark SQL** (`regexp_extract`, `regexp_replace`) — they run inside the JVM, without Python per row, so the whole dump is processed **in one pass** in **29 minutes**. The output (Parquet, 2.3 GB) has two kinds of rows:

| Kind | Count | Content |
|---|---|---|
| articles | 7,259,919 | title, short description, infobox type and fields, abstract, health/safety section, categories, disambiguation flag |
| redirects | 12,097,295 | title → target; 421 of them are "R from E number" redirects (`E951` → *Aspartame*) |

| Statistic of the processed dump | Count |
|---|---|
| disambiguation pages | 453,228 |
| articles with an infobox | 5,220,157 |
| articles with a health / safety / controversy section | 97,477 |
| most common infobox types | settlement (465,781), speciesbox (394,568), person (373,288), officeholder (266,996), football biography (215,008), film (166,913), album (154,409), musical artist (109,450) |
| articles with `Infobox company` | 78,877 |

## 3. Regular expressions on the wiki data

[spark_wiki.py](https://github.com/xfarkasp/vinf_crawler/blob/main/spark_wiki.py) — example values from the article *Milka*:

| Field | Regex (Java syntax, Spark SQL) | Example |
|---|---|---|
| short description | `(?i)\{\{\s*short description\s*\\|\s*([^}\n\|]*)` | European chocolate brand |
| infobox type | `(?i)\{\{\s*(infobox[ _][^\|\n}<]*\|chembox\|drugbox\|...)` | brand |
| infobox field | `(?im)^\s*\\|\s*(?:currentowner\|owner\|owners)\s*=\s*([^\n]*)` | Mondelez International |
| founded year | `\b(1[5-9][0-9]{2}\|20[0-9]{2})\b` on the `founded`/`introduced` value | 1901 |
| categories | `\[\[\s*[Cc]ategory\s*:\s*([^\]\|]+)` (`regexp_extract_all`) | Chocolate brands, … |
| disambiguation | `(?i)\{\{\s*(disambiguation\|disambig\|dab\|surname\|...)\s*(\\|\|\}\})` | *Ferrero* → yes |
| E-number redirect | `(?i)\{\{\s*R from E number` | `E951` → Aspartame |
| health section | a `== … ==`/`=== … ===` heading containing *safety, health, toxicity, controversy, criticism, environmental impact* and the text up to the next heading | *Aspartame*: "Safety and health effects" |
| abstract | lead section (text before the first `==`) → markup removed → first line of ≥ 40 characters | "Milka is a brand of chocolate confection …" |

Wikitext → plain text is also done with regex (comments, references, nested templates `{{…}}` removed level by level, tables, files; links `[[target|text]]` → *text*):

```python
text = F.regexp_replace(text, r"(?s)<!--.*?-->", "")
text = F.regexp_replace(text, r"(?is)<ref[^>]*>.*?</ref>", "")
for _ in range(template_depth):      # templates nest: remove the innermost {{...}} each pass
    text = F.regexp_replace(text, r"\{\{[^{}]*\}\}", "")
text = F.regexp_replace(text, r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", "$1")
```

## 4. The join

[spark_join.py](https://github.com/xfarkasp/vinf_crawler/blob/main/spark_join.py). The crawled data has four fields that name things Wikipedia has articles about:

| Join type | Product field | Example | Why |
|---|---|---|---|
| brand | `brands` | *Milka* → Milka (owner Mondelez International) | who makes / owns the product (project questions 4, 5) |
| additive | `additives`, `additive_names` | *E951* → Aspartame | what an additive is, its safety (questions 2, 7) |
| ingredient | `ingredients_text` (English) | *palm oil* → Palm oil | context for ingredients (question 5) |
| country | `countries` | *France* → France | where the product is sold |

**Logic:**
1. Every product value and every Wikipedia title (articles and redirect titles) is **normalised the same way**: lowercase, accents removed, non-alphanumerics → space. *Coca Cola*, *COCA COLA* and *Coca-Cola* all give `coca cola`.
1. **key → page** through article titles and redirects. For additives Wikipedia's "R from E number" redirects give an exact match (`E322` → *Lecithin*). Brands also try `<brand> (brand)` and `<brand> (company)`, because e.g. *Ferrero* is a disambiguation page and Kinder's article is *Kinder (brand)*.
1. **Validation** — a match counts only if the article fits the type; disambiguation pages never count:
  - brand → infobox `company`/`brand`/`retail`…, or a description/category with *brand, company, manufacturer…*
  - additive → an E-number redirect, an additive category (*E-number additives, sweeteners…*) or a chemical infobox
  - ingredient → a food/plant/chemical infobox or a food category; never a place, person or organisation
  - country → infobox `country`
1. Per product value the best candidate is kept (exact name before variants, direct article before redirect).

The checks were tuned on samples of the results: without them brands matched *Cappuccino* and *Cola*, and the ingredient *riga* matched the city of Riga (its category "NUTS 3 statistical regions" contains the word *nuts*).

```python
candidates = (values.join(title_map(wiki), "key")                 # key -> page (article or redirect)
              .join(articles, F.col("page") == articles.title)
              .filter(~F.col("is_disambiguation")))
# keep only matches that fit the join type (VALID_TARGET), then the best one per value
best = Window.partitionBy("barcode", "join_type", "value").orderBy("priority", "via_redirect")
links = valid.withColumn("rank", F.row_number().over(best)).filter("rank = 1")
...
enriched = products.join(linked, "barcode", how="left")          # products without a match are kept
```

### Examples of joined data

| Product | Join type | Product value | Wikipedia page | Joined data (from the article) |
|---|---|---|---|---|
| Kinder (8000500446706) | brand | Kinder | [Kinder (brand)](https://en.wikipedia.org/wiki/Kinder_(brand)) | owner: Ferrero SpA; Kinder (German for "children") is an Italian brand of chocolate produced by Italian … |
| Milka (7622202260810) | brand | Milka | [Milka](https://en.wikipedia.org/wiki/Milka) | owner: Mondelez International, (2012–present); founded 1901 |
| Nutella (8000500193389) | brand | Ferrero | [Ferrero SpA](https://en.wikipedia.org/wiki/Ferrero_SpA) | Italian multinational food corporation; founded 1946 by Pietro Ferrero; Italy |
| Cola (3660992012527) | additive | Aspartame | [Aspartame](https://en.wikipedia.org/wiki/Aspartame) | Artificial non-saccharide sweetener; *Safety and health effects*: The safety of aspartame has been studied since its discovery, and it is a rigorously tested food … |
| Nutella (8000500179864) | additive | Lecithins | [Lecithin](https://en.wikipedia.org/wiki/Lecithin) | Lecithin is a generic term to designate a group of yellow-brownish fatty substances occurring in animal and plant … |
| Nutella (8000500398418) | ingredient | palm oil | [Palm oil](https://en.wikipedia.org/wiki/Palm_oil) | *Nutrition, composition and health*: Palm oil is a food staple in many cuisines, contributing significant calories and plant lipids. … |
| Nutella (0012970422003) | country | France | [France](https://en.wikipedia.org/wiki/France) | capital: Paris (de facto); Country primarily in Western Europe |

## 5. How many unique wiki pages were joined

**3,780 unique Wikipedia pages** are joined to the products (the join itself takes 83 s).

| Join type | Products with a value | Products joined | Distinct values | Values joined | Unique wiki pages | Most joined pages |
|---|---|---|---|---|---|---|
| brand | 93,524 | 35,094 (38%) | 21,062 | 2,589 | **1,572** | Carrefour, Lidl, Auchan, Nestlé, Mercadona |
| additive | 44,163 | 43,220 (98%) | 839 | 542 | **343** | Citric acid, Lecithin, Vitamin C, Mono- and diglycerides of fatty acids, Xanthan gum |
| ingredient | 34,532 | 30,629 (89%) | 53,680 | 4,422 | **1,980** | Salt, Flavoring, Milk, Citric acid, Rapeseed oil |
| country | 99,284 | 99,115 (100%) | 228 | 170 | **162** | France, Germany, United Kingdom, Spain, Morocco |
| **all** | 99,328 | 99,246 (99.9%) |   |   | **3,780** |   |

Precision (manual check of random samples): brands 37/40 correct — the errors are same-name companies from other industries (*Crane* → Crane Currency, *Amita* → AMITA Health); additives and ingredients almost always correct (synonyms work through redirects: *cellulose gum* → Carboxymethyl cellulose, *nuoc-mâm* → Fish sauce, *Mondelēz* → Mondelez International).

## 6. Interesting Wikipedia attributes

| Attribute | Why it is interesting for the project | Example |
|---|---|---|
| `short_description` | one-line definition of what an additive / ingredient / brand is | Aspartame: Artificial non-saccharide sweetener |
| `abstract` | first paragraph — readable explanation shown with a search result (project question 2) | Lecithin: Lecithin is a generic term to designate a group of yellow-brownish fatty … |
| `owner / parent` | which corporation owns a brand (question 4: Milka → Mondelez) | Milka: Mondelez International, (2012–present) |
| `founded_year, founder, country` | origin of a manufacturer | Ferrero SpA: 1946, Pietro Ferrero, Italy |
| `industry` | what the company does; also used to reject same-name non-food companies | Ferrero SpA: Food |
| `health_section` | safety, health effects, controversies (questions 5 and 7) | Monosodium glutamate: *Safety*: MSG is generally recognized as safe to eat. A popular belief is that … |
| `categories` | e.g. *E-number additives*, *Chocolate brands* — classification for filters and for validating the join | Kinder (brand): Ferrero SpA brands, German confectionery, Italian confectionery |
| `infobox_type` | kind of entity (company, brand, chembox, country) — used to validate matches | Aspartame: chembox |
| `capital` | basic country fact for the *countries where sold* | France: Paris (de facto) |

## 7. Running

```bash
brew install openjdk@17 && pip install -r requirements.txt     # Java 17, pyspark==3.5.3
sh wiki_dump/download.sh                                      # English Wikipedia dump (~26 GB)
python spark_extract_products.py                              # crawled HTML -> spark_output/products
python spark_wiki.py                                          # dump -> spark_output/wiki_pages
python spark_join.py                                          # -> spark_output/joined
python spark_show.py product 3017620422003                    # a product and its Wikipedia links
python -m unittest discover tests -v                          # 32 tests (incl. 10 Spark tests)

# the same with spark-submit, as in the lecture (e.g. on the cluster, with HDFS paths)
spark-submit --master spark://host:7077 --packages com.databricks:spark-xml_2.12:0.18.0 \
    spark_wiki.py --input hdfs:///user/x/enwiki-latest-pages-articles.xml.bz2 --output hdfs:///user/x/wiki_pages
```

**Tests:** [tests/test_spark.py](https://github.com/xfarkasp/vinf_crawler/blob/main/tests/test_spark.py) runs the Spark functions on small wikitext samples: infobox fields, abstract with nested templates, health section, categories, disambiguation, E-number redirects, join-key normalisation and the join values of a product.
