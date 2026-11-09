"""Shared helpers for the Spark jobs (spark_extract_products.py, spark_wiki.py, spark_join.py).

The jobs run locally (`local[*]`, all CPU cores) by default. On a cluster either
start them with spark-submit (its --master is respected) or set SPARK_MASTER
(e.g. spark://host:7077), and pass HDFS paths as --input/--output.
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlparse

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

# spark-xml reads the Wikipedia XML dump (Spark 3.5 has no built-in XML reader).
SPARK_XML_PACKAGE = "com.databricks:spark-xml_2.12:0.18.0"

# Accented letters -> ASCII, so "Nestlé" and "Nestle" get the same join key.
_ACCENTED = "áàâäãåāăąçćčďđéèêëēėęěíìîïīįľĺłñńňóòôöõøōőŕřśšşťúùûüūůűųýÿžźż"
_PLAIN = "aaaaaaaaacccddeeeeeeeeiiiiiilllnnnoooooooorrssstuuuuuuuuyyzzz"
assert len(_ACCENTED) == len(_PLAIN)


def create_spark(app_name: str, xml: bool = False, conf: dict = None) -> SparkSession:
    """SparkSession for local runs (all cores) or a cluster (SPARK_MASTER)."""
    # Local runs on macOS: use Homebrew's Java 17 and this venv's Python for workers.
    if "JAVA_HOME" not in os.environ and os.path.isdir("/opt/homebrew/opt/openjdk@17"):
        os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(app_name).config("spark.sql.shuffle.partitions", "64")
    # Started by spark-submit (it passes its JVM to Python via PYSPARK_GATEWAY_PORT): its
    # --master and --driver-memory already apply. Started with plain python: SPARK_MASTER,
    # else all local cores.
    if "PYSPARK_GATEWAY_PORT" not in os.environ:
        builder = (builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
                   .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "12g")))
    if xml:
        builder = builder.config("spark.jars.packages", SPARK_XML_PACKAGE)
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stats_path(output: str) -> str:
    """Local file for a job's statistics JSON: next to a local output, or in the
    current directory when the output is on HDFS (Python cannot open() hdfs:// paths)."""
    name = output.rstrip("/") + "_stats.json"
    return name if urlparse(output).scheme in ("", "file") else os.path.basename(name)


def normalize_key(column: Column) -> Column:
    """Join key: lowercase, accents removed, every run of non-alphanumerics -> one space.

    'Nestlé' -> 'nestle', 'Mondelez International' -> 'mondelez international',
    'E-951' -> 'e 951'. The same expression is used on the product side and on the
    Wikipedia side, so both normalise identically.
    """
    folded = F.translate(F.lower(column), _ACCENTED, _PLAIN)
    return F.trim(F.regexp_replace(folded, r"[^a-z0-9]+", " "))
