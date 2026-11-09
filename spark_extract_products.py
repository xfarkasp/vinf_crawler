"""Distributed version of extractor.py: extract products from the crawled HTML with Spark.

The stored pages (html/products/<shard>/<barcode>.html.gz) are split into
partitions and processed in parallel by Spark workers. Each
worker runs exactly the same regex extractor as the first submission
(extractor.extract_product), so the logic stays unit-tested in one place.

Reading 99k small files through Spark's Hadoop file layer (sc.binaryFiles) is
slow - the "small files problem": per-file overhead dominates (46 s just to read
11k files, vs 20 s for the whole sequential extraction). For a local disk the
driver therefore only lists the paths and the workers open the files directly
(--reader paths, the default for local paths). On HDFS / a cluster, where workers
cannot see the driver's disk, --reader binary uses sc.binaryFiles.

Output: Parquet (for the join job) and JSON Lines (readable, same format as
products_data.jsonl).

Usage:
  python spark_extract_products.py
  python spark_extract_products.py --input "hdfs:///user/x/html/products/*/*.html.gz" \
                                   --output hdfs:///user/x/spark_output/products
"""

from __future__ import annotations

import argparse
import glob
import gzip
import os
import time
from urllib.parse import urlparse

from pyspark.sql.types import (ArrayType, DoubleType, StringType, StructField,
                               StructType)

from spark_common import create_spark

DEFAULT_INPUT = "html/products/*/*.html.gz"
DEFAULT_OUTPUT = "spark_output/products"

NUTRITION = StructType([StructField(name, DoubleType()) for name in
                        ["energy-kcal", "fat", "carbohydrates", "sugars", "proteins"]])
PRODUCT_SCHEMA = StructType([
    StructField("url", StringType()),
    StructField("barcode", StringType()),
    StructField("product_name", StringType()),
    StructField("brands", ArrayType(StringType())),
    StructField("quantity", StringType()),
    StructField("categories", ArrayType(StringType())),
    StructField("labels", ArrayType(StringType())),
    StructField("countries", ArrayType(StringType())),
    StructField("nutri_score", StringType()),
    StructField("eco_score", StringType()),
    StructField("ingredients_text", StringType()),
    StructField("ingredients_language", StringType()),
    StructField("additives", ArrayType(StringType())),
    StructField("additive_names", ArrayType(StringType())),
    StructField("allergens", ArrayType(StringType())),
    StructField("traces", ArrayType(StringType())),
    StructField("nutrition_100g", NUTRITION),
])


def _to_row(page: str) -> tuple:
    import extractor  # shipped to the workers with addPyFile
    product = extractor.extract_product(page)
    return tuple(product.get(field.name) for field in PRODUCT_SCHEMA.fields)


def extract_partition(files):
    """Worker, --reader binary: (path, gzipped bytes) pairs from sc.binaryFiles."""
    for _, content in files:
        try:
            page = gzip.decompress(content).decode("utf-8")
        except (OSError, UnicodeDecodeError):
            continue  # unreadable file: skip it, like extractor.py does
        yield _to_row(page)


def extract_paths_partition(paths):
    """Worker, --reader paths: open the files directly (local or shared disk)."""
    for path in paths:
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                page = f.read()
        except (OSError, UnicodeDecodeError):
            continue
        yield _to_row(page)


def is_local(path: str) -> bool:
    return urlparse(path).scheme in ("", "file")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--partitions", type=int, default=80)
    parser.add_argument("--reader", choices=["auto", "paths", "binary"], default="auto",
                        help="paths: workers open local files; binary: sc.binaryFiles (HDFS); "
                             "auto: paths for local input, binary otherwise")
    args = parser.parse_args()

    started = time.time()
    spark = create_spark("extract-products")
    spark.sparkContext.addPyFile(os.path.join(os.path.dirname(os.path.abspath(__file__)), "extractor.py"))

    reader = args.reader if args.reader != "auto" else ("paths" if is_local(args.input) else "binary")
    sc = spark.sparkContext
    if reader == "paths":
        paths = sorted(glob.glob(urlparse(args.input).path))
        rows = sc.parallelize(paths, args.partitions).mapPartitions(extract_paths_partition)
    else:
        rows = sc.binaryFiles(args.input, minPartitions=args.partitions).mapPartitions(extract_partition)
    products = spark.createDataFrame(rows, PRODUCT_SCHEMA)
    products = products.dropDuplicates(["barcode"])

    products.write.mode("overwrite").parquet(args.output)
    spark.read.parquet(args.output).write.mode("overwrite").json(args.output + "_json")

    count = spark.read.parquet(args.output).count()
    print(f"Extracted {count} products ({reader} reader) -> {args.output} (Parquet) and "
          f"{args.output}_json (JSON Lines) in {time.time() - started:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main()
