"""Unit tests for the regex extractors (extractor.py) and link extraction (crawler.py).

Fixtures are real crawled pages in tests/fixtures/.

* 20 product pages are checked against expected values produced by an
  independent DOM-based parser (BeautifulSoup) used in an early version of the
  project (tests/fixtures/expected_products.json). The regex extractor must
  give the same result for every field.
* Edge cases found while developing the extractor have hand-written
  expectations, checked against the raw HTML.

Run from the project root:
  python -m unittest discover tests -v
"""

import gzip
import json
import os
import unittest

import crawler
import extractor
from indexer import tokenize
from search import Query

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def product_page(barcode: str) -> str:
    with gzip.open(os.path.join(FIXTURES, "products", f"{barcode}.html.gz"), "rt", encoding="utf-8") as f:
        return f.read()


def listing_page(number: int) -> str:
    with gzip.open(os.path.join(FIXTURES, "listings", f"{number}.html.gz"), "rt", encoding="utf-8") as f:
        return f.read()


class TestAgainstIndependentParser(unittest.TestCase):
    """20 pages: regex results must equal the DOM-based parser's results."""

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(FIXTURES, "expected_products.json"), encoding="utf-8") as f:
            cls.expected = json.load(f)

    def test_twenty_pages(self):
        self.assertGreaterEqual(len(self.expected), 20)
        for barcode, fields in self.expected.items():
            record = extractor.extract_product(product_page(barcode))
            for field, value in fields.items():
                with self.subTest(barcode=barcode, field=field):
                    self.assertEqual(record[field], value)

    def test_barcode_and_url(self):
        for barcode in self.expected:
            record = extractor.extract_product(product_page(barcode))
            with self.subTest(barcode=barcode):
                self.assertEqual(record["barcode"], barcode)
                self.assertTrue(record["url"].startswith(f"https://world.openfoodfacts.org/product/{barcode}"))


class TestEdgeCases(unittest.TestCase):
    def test_less_than_sign_in_nutrition_value(self):
        # The page shows "< 0.5 g" with a raw "<", which must not be taken for a tag.
        nutrition = extractor.extract_product(product_page("6111184001074"))["nutrition_100g"]
        self.assertEqual(nutrition["fat"], 0.5)
        self.assertEqual(nutrition["proteins"], 0.5)

    def test_decimal_comma_in_quantity(self):
        record = extractor.extract_product(product_page("6111251420166"))
        self.assertEqual(record["quantity"], "1,5 l")
        # Heading "Eau – Aïn lfrane – 1,5 l": brand and quantity are stripped.
        self.assertEqual(record["product_name"], "Eau")

    def test_ingredients_in_another_language(self):
        record = extractor.extract_product(product_page("00004465"))
        self.assertEqual(record["ingredients_language"], "French")
        self.assertTrue(record["ingredients_text"].startswith("Isolat de protéines de soja"))
        self.assertNotIn("French", record["ingredients_text"])

    def test_english_ingredients_language(self):
        record = extractor.extract_product(product_page("6111242100992"))
        self.assertEqual(record["ingredients_language"], "English")

    def test_product_without_name_shows_barcode(self):
        self.assertIsNone(extractor.extract_product(product_page("4250107660553"))["product_name"])

    def test_product_without_name_shows_quantity(self):
        record = extractor.extract_product(product_page("5902884465935"))
        self.assertIsNone(record["product_name"])
        self.assertEqual(record["quantity"], "450 g")

    def test_heading_without_brand_keeps_no_trailing_dash(self):
        # Without a brand the page heading is "Pizza cheese – ".
        self.assertEqual(extractor.extract_name('<h1 itemprop="name">Pizza cheese – </h1>', None, None),
                         "Pizza cheese")

    def test_additive_names(self):
        record = extractor.extract_product(product_page("6111184001074"))
        self.assertEqual(record["additive_names"], ["Citric acid", "Pectins"])
        self.assertEqual(len(record["additives"]), len(record["additive_names"]))

    def test_green_score_a_plus(self):
        self.assertEqual(extractor.extract_product(product_page("3760020508012"))["eco_score"], "A+")

    def test_clean_text(self):
        # Entities are decoded and every whitespace run (incl. &nbsp;) becomes one space.
        self.assertEqual(extractor.clean_text("<span>\n  &lt; 0.5&nbsp;g </span>"), "< 0.5 g")
        self.assertEqual(extractor.clean_text("< 0.5 g\n</span>"), "< 0.5 g")


class TestLinkExtraction(unittest.TestCase):
    def test_fifty_product_links_on_listing_page(self):
        links = crawler.extract_product_links(listing_page(2))
        self.assertEqual(len(links), 50)
        self.assertEqual(len({crawler.product_code(u) for u in links}), 50)
        for url in links:
            self.assertRegex(url, r"^https://world\.openfoodfacts\.org/product/\d{4,}(/|$)")

    def test_next_page(self):
        self.assertEqual(
            crawler.extract_next_page(listing_page(2), "https://world.openfoodfacts.org/2"),
            "https://world.openfoodfacts.org/3",
        )
        self.assertIsNone(crawler.extract_next_page("<html>last page</html>", "https://world.openfoodfacts.org/9"))

    def test_only_product_urls_match(self):
        html = ('"url":"https://world.openfoodfacts.org/product/3017620422003/nutella" '
                '<a href="/products">all</a> <a href="/cgi/product.pl?type=add">add</a> '
                '<script src="/js/product-search.js"></script> "/product/12/x"')
        self.assertEqual(crawler.extract_product_links(html),
                         ["https://world.openfoodfacts.org/product/3017620422003/nutella"])

    def test_one_url_per_barcode_prefers_slug(self):
        html = '/product/3017620422003 and /product/3017620422003/nutella'
        self.assertEqual(crawler.extract_product_links(html),
                         ["https://world.openfoodfacts.org/product/3017620422003/nutella"])

    def test_guess_next_page(self):
        self.assertEqual(crawler.guess_next_page("https://world.openfoodfacts.org/"),
                         "https://world.openfoodfacts.org/2")
        self.assertEqual(crawler.guess_next_page("https://world.openfoodfacts.org/41"),
                         "https://world.openfoodfacts.org/42")

    def test_html_path_sharding(self):
        self.assertEqual(crawler.html_path_for("https://world.openfoodfacts.org/product/3017620422003/nutella"),
                         os.path.join("html", "products", "301", "3017620422003.html.gz"))
        self.assertEqual(crawler.html_path_for("https://world.openfoodfacts.org/7"),
                         os.path.join("html", "listings", "7.html.gz"))


class TestTokenizer(unittest.TestCase):
    def test_tokenize(self):
        self.assertEqual(tokenize("Crème brûlée, with SUGAR and 100 g of E322"),
                         ["creme", "brulee", "sugar", "e322"])


class TestQueryParsing(unittest.TestCase):
    def test_operators_and_filters(self):
        query = Query("Hazelnut spread +vegan -palm nutri:a,b eco:a+ sugars<5 proteins>10")
        self.assertEqual(query.terms, ["hazelnut", "spread", "vegan"])
        self.assertEqual(query.required, {"vegan"})
        self.assertEqual(query.excluded, {"palm"})
        self.assertEqual(query.grades, {"nutri_score": {"A", "B"}, "eco_score": {"A+"}})
        self.assertEqual(query.nutrients, [("sugars", "<", 5.0), ("proteins", ">", 10.0)])

    def test_prefix_exclusion(self):
        query = Query("hazelnut spread -palm* -soy")
        self.assertEqual(query.excluded_prefixes, {"palm"})
        self.assertEqual(query.excluded, {"soy"})
        self.assertEqual(query.terms, ["hazelnut", "spread"])

    def test_filters_accept_document(self):
        doc = {"nutri_score": "A", "eco_score": "B", "nutrition_100g": {"sugars": 3.0, "proteins": 12.0}}
        self.assertTrue(Query("bar nutri:a sugars<5 proteins>10").accepts(doc))
        self.assertFalse(Query("bar nutri:b").accepts(doc))
        self.assertFalse(Query("bar sugars<3").accepts(doc))
        self.assertFalse(Query("bar fat<3").accepts(doc))  # unknown value does not pass a filter


if __name__ == "__main__":
    unittest.main()
