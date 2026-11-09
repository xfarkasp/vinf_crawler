"""Unit tests for the Spark jobs: Wikipedia regex extraction, join keys, join values.

They run the real Spark functions on small hand-written samples (local Spark,
a few seconds to start). Skipped when PySpark or Java is not available.

Run from the project root:
  python -m unittest tests.test_spark -v
"""

import os
import shutil
import unittest

try:
    import pyspark  # noqa: F401
    HAVE_SPARK = bool(shutil.which("java") or os.environ.get("JAVA_HOME")
                      or os.path.isdir("/opt/homebrew/opt/openjdk@17"))
except ImportError:
    HAVE_SPARK = False

MILKA = """{{short description|European chocolate brand}}
{{About|the confectionery|the given name|Milka (given name)}}
{{Infobox brand
| name         = Milka
| currentowner = [[Mondelez International]]<br>(2012–present)
| introduced   = {{start date|1901}}
| country      = [[Switzerland]]
}}
'''Milka''' is a brand of [[chocolate]] confection which originated in [[Switzerland]] in 1901.<ref>{{cite web|title=x}}</ref>

== History ==
Text.

== Health concerns ==
Milka contains [[palm oil|palm fat]] and a lot of sugar.

== References ==
[[Category:Chocolate brands]]
[[Category:Mondelez International brands]]
"""

COUNTRY = """{{Infobox country
| conventional_long_name = Republic of Example
| capital = [[Example City]]
| leader = {{nowrap|{{flagicon|EX}} [[Somebody]]}}
}}
'''Exampleland''' is a country in [[Europe]] that is used only in unit tests of the project.
"""

COMPANY = """{{Infobox company
| name    = Ferrero SpA
| parent  =
| fate    = Merged
| founded = {{start date and age|1946|05|14}}, in [[Alba, Piedmont|Alba]]
}}
'''Ferrero SpA''' ({{IPA|it|ferˈrɛːro}}, {{IPA|en|fəˈrɛəroʊ}}) is an Italian multinational company.
"""

KINDER = """'''Kinder''' ({{IPA|de|ˈkɪndɐ}}; German for "children") is an Italian brand of chocolate."""

DISAMBIGUATION = """'''Ferrero''' may refer to:
* [[Ferrero (surname)]], a surname
* [[Ferrero SpA]], an Italian manufacturer of chocolate
{{disambiguation}}"""


@unittest.skipUnless(HAVE_SPARK, "PySpark / Java not available")
class SparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from spark_common import create_spark
        cls.spark = create_spark("unit-tests", conf={"spark.sql.codegen.wholeStage": "false",
                                                     "spark.ui.enabled": "false"})

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestNormalizeKey(SparkTestCase):
    def test_variants_get_the_same_key(self):
        from pyspark.sql import functions as F
        from spark_common import normalize_key
        df = self.spark.createDataFrame([("Coca-Cola",), ("COCA COLA",), ("Nestlé",), (" E-951 ",)], ["v"])
        keys = [r.k for r in df.select(normalize_key(F.col("v")).alias("k")).collect()]
        self.assertEqual(keys, ["coca cola", "coca cola", "nestle", "e 951"])


class TestWikiExtraction(SparkTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from spark_wiki import PAGE_SCHEMA, build
        pages = [
            ("Milka", "0", None, (MILKA,)),
            ("Exampleland", "0", None, (COUNTRY,)),
            ("Ferrero SpA", "0", None, (COMPANY,)),
            ("Kinder (brand)", "0", None, (KINDER,)),
            ("Ferrero", "0", None, (DISAMBIGUATION,)),
            ("E951", "0", ("Aspartame",), ("#REDIRECT [[Aspartame]] {{Redirect category shell|{{R from E number}}}}",)),
            ("Lecithins", "0", ("Lecithin#Uses",), ("#REDIRECT [[Lecithin#Uses]]",)),
        ]
        rows = [(t, ns, (r[0],) if r else None, (text,)) for t, ns, r, text in pages]
        df = cls.spark.createDataFrame(rows, PAGE_SCHEMA)
        cls.rows = {r.title: r for r in build(df).collect()}

    def test_infobox_brand_fields(self):
        milka = self.rows["Milka"]
        self.assertEqual(milka.kind, "article")
        self.assertEqual(milka.infobox_type, "brand")
        self.assertEqual(milka.short_description, "European chocolate brand")
        self.assertEqual(milka.owner, "Mondelez International, (2012–present)")
        self.assertEqual(milka.country, "Switzerland")
        self.assertEqual(milka.founded_year, "1901")
        self.assertFalse(milka.is_disambiguation)

    def test_abstract_is_clean_text(self):
        self.assertEqual(self.rows["Milka"].abstract,
                         "Milka is a brand of chocolate confection which originated in Switzerland in 1901.")

    def test_nested_infobox_is_not_the_abstract(self):
        self.assertTrue(self.rows["Exampleland"].abstract.startswith("Exampleland is a country in Europe"))
        self.assertEqual(self.rows["Exampleland"].capital, "Example City")

    def test_health_section_and_categories(self):
        milka = self.rows["Milka"]
        self.assertEqual(milka.health_section_title, "Health concerns")
        self.assertEqual(milka.health_section, "Milka contains palm fat and a lot of sugar.")
        self.assertEqual(milka.categories, ["Chocolate brands", "Mondelez International brands"])

    def test_empty_infobox_field_does_not_take_the_next_line(self):
        ferrero = self.rows["Ferrero SpA"]
        self.assertIsNone(ferrero.parent)
        self.assertEqual(ferrero.founded_year, "1946")

    def test_removed_pronunciation_leaves_no_empty_brackets(self):
        self.assertEqual(self.rows["Ferrero SpA"].abstract, "Ferrero SpA is an Italian multinational company.")
        self.assertEqual(self.rows["Kinder (brand)"].abstract,
                         'Kinder (German for "children") is an Italian brand of chocolate.')

    def test_disambiguation_page(self):
        self.assertTrue(self.rows["Ferrero"].is_disambiguation)

    def test_redirects(self):
        self.assertEqual(self.rows["E951"].kind, "redirect")
        self.assertEqual(self.rows["E951"].target, "Aspartame")
        self.assertTrue(self.rows["E951"].from_e_number)
        self.assertEqual(self.rows["Lecithins"].target, "Lecithin")  # section anchor removed
        self.assertFalse(self.rows["Lecithins"].from_e_number)


class TestJoinValues(SparkTestCase):
    def test_product_values(self):
        from spark_extract_products import PRODUCT_SCHEMA
        from spark_join import product_values
        product = {f.name: None for f in PRODUCT_SCHEMA.fields}
        product.update(barcode="1", brands=["Kinder"], additives=["E322"], additive_names=["Lecithins"],
                       countries=["France"], ingredients_language="English",
                       ingredients_text="Sugar, palm oil, hazelnuts (13%), emulsifier: lecithins (soya)")
        df = self.spark.createDataFrame([tuple(product[f.name] for f in PRODUCT_SCHEMA.fields)], PRODUCT_SCHEMA)
        rows = {(r.join_type, r.candidate, r.priority) for r in product_values(df).collect()}
        self.assertIn(("brand", "Kinder", 0), rows)
        self.assertIn(("brand", "Kinder (brand)", 1), rows)
        self.assertIn(("brand", "Kinder (company)", 2), rows)
        self.assertIn(("additive", "E322", 0), rows)
        self.assertIn(("additive", "Lecithins", 1), rows)
        self.assertIn(("country", "France", 0), rows)
        ingredients = {c for t, c, _ in rows if t == "ingredient"}
        self.assertEqual(ingredients, {"sugar", "palm oil", "hazelnuts", "emulsifier", "lecithins", "soya"})


if __name__ == "__main__":
    unittest.main()
