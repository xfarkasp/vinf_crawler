"""Regex-based extractor for crawled OpenFoodFacts product pages.

Reads the raw HTML stored by crawler.py (html/products/**/*.html.gz) and writes
one JSON record per product to products_data.jsonl. Only regular expressions
are used - no HTML parser / XPath / DOM selectors. The patterns anchor on
visible labels ("Brands:", "Allergens:"), link targets (/facets/brands/...),
image names (nutriscore-e.svg) and text patterns (E322) rather than on the
exact nesting of the page.

Usage:
  python extractor.py                 # all stored pages
  python extractor.py --limit 20      # first 20 pages (quick check)
"""

from __future__ import annotations

import argparse
import glob
import gzip
import html as html_lib
import json
import os
import re
from typing import Optional

HTML_GLOB = os.path.join("html", "products", "*", "*.html.gz")
OUTPUT_FILE = "products_data.jsonl"

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# A tag starts with a letter, "/" or "!" - so a raw "< 0.5 g" is kept as text.
TAG_RE = re.compile(r"<[A-Za-z/!][^>]*>")
WHITESPACE_RE = re.compile(r"\s+")

# <meta property="og:url" content="https://world.openfoodfacts.org/product/...">
CANONICAL_URL_RE = re.compile(r'<meta\s+property="og:url"\s+content="([^"]+)"')
BARCODE_RE = re.compile(r"/product/(\d{4,})")
# <h1 ... itemprop="name">Nutella – 400 g</h1>
NAME_RE = re.compile(r'<h1[^>]*itemprop="name"[^>]*>(.*?)</h1>', re.DOTALL)
TRAILING_DASH_RE = re.compile(r"(?:\s*–)+\s*$")
# A characteristics field: <span class="field">Brands: </span> <values> </p>
FIELD_RE_TEMPLATE = r'<span class="field">\s*{label}\s*:?\s*</span>(.*?)</p>'
LINK_TEXT_RE = re.compile(r"<a\b[^>]*>(.*?)</a>", re.DOTALL)
# Score badges are images named e.g. nutriscore-e-new-en.svg, green-score-a-plus.svg
NUTRISCORE_RE = re.compile(r"nutriscore-([a-e])[-.]")
ECOSCORE_RE = re.compile(r"(?:green-score|ecoscore|environmental-score)-(a-plus|[a-f])[-.]")
# Ingredients panel and its text blocks
# The panel ends where the per-ingredient list starts, or (when there is no
# list) at its own end comment "<!-- end ... panel_id: ingredients -->".
INGREDIENTS_PANEL_RE = re.compile(
    r'id="panel_ingredients_content"(.*?)(?:panel_id: ingredients_list|panel_id: ingredients -->)',
    re.DOTALL,
)
PANEL_TEXT_RE = re.compile(r'<div class="panel_text">(.*?)</div>', re.DOTALL)
# Ingredients in another language than the interface carry a language label:
# <!-- text is in a different language than the interface --> <strong>French: </strong>
OTHER_LANGUAGE_RE = re.compile(
    r"different language than the interface\s*-->\s*<strong>\s*([^<:]+?)\s*:?\s*</strong>(.*)", re.DOTALL
)
LABELLED_TEXT_RE_TEMPLATE = r"<strong>\s*{label}\s*:?\s*</strong>(.*?)</div>"
ALLERGEN_SPAN_RE = re.compile(r'<span class="allergen">(.*?)</span>', re.DOTALL)
# Additive headings: "E322 - Lecithins"; fallback: E-numbers in ingredient text
ADDITIVE_HEADING_RE = re.compile(r"<h4[^>]*>\s*(E\d{3,4}[a-z]?)\s+-\s+([^<]*?)\s*</h4>", re.IGNORECASE)
E_NUMBER_RE = re.compile(r"\bE\s?(\d{3,4}[a-z]?)\b", re.IGNORECASE)
# Nutrition facts table
NUTRITION_TABLE_RE = re.compile(r'<table[^>]*aria-label="Nutrition facts"[^>]*>(.*?)</table>', re.DOTALL)
HEADER_CELL_RE = re.compile(r"<th\b[^>]*>(.*?)</th>", re.DOTALL)
ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.DOTALL)
CELL_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.DOTALL)
PER_100_RE = re.compile(r"100\s?(?:g|ml)", re.IGNORECASE)
NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")
KCAL_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*kcal", re.IGNORECASE)

# Row label in the nutrition table -> key in nutrition_100g
NUTRIENT_ROWS = {
    "energy": "energy-kcal",
    "energy (kcal)": "energy-kcal",
    "fat": "fat",
    "carbohydrates": "carbohydrates",
    "sugars": "sugars",
    "proteins": "proteins",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def clean_text(fragment: Optional[str]) -> Optional[str]:
    """Strip tags, decode entities (&nbsp; &amp; ...), collapse whitespace."""
    if fragment is None:
        return None
    text = html_lib.unescape(TAG_RE.sub("", fragment))
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text or None


def dedupe(items: list) -> list:
    return list(dict.fromkeys(items))


def to_float(text: str) -> Optional[float]:
    match = NUMBER_RE.search(text)
    return float(match.group().replace(",", ".")) if match else None


def split_values(fragment: str) -> list:
    """Values of a field: link texts if there are links, else comma-separated text."""
    values = [clean_text(v) for v in LINK_TEXT_RE.findall(fragment)]
    if not values:
        values = [clean_text(v) for v in (clean_text(fragment) or "").split(",")]
    return dedupe([v for v in values if v])


def strip_suffix(text: str, suffix: str) -> str:
    if text.lower().endswith(suffix.lower()) and len(text) > len(suffix):
        return text[: -len(suffix)].strip()
    return text


# ---------------------------------------------------------------------------
# Field extractors
# ---------------------------------------------------------------------------


def extract_url(page: str) -> Optional[str]:
    match = CANONICAL_URL_RE.search(page)
    return html_lib.unescape(match.group(1)) if match else None


def extract_field(page: str, label: str) -> Optional[list]:
    """A characteristics field by its visible label, e.g. 'Brands'."""
    match = re.search(FIELD_RE_TEMPLATE.format(label=re.escape(label)), page, re.DOTALL)
    return (split_values(match.group(1)) or None) if match else None


def extract_quantity(page: str) -> Optional[str]:
    # Not split on commas: "1,5 l" is a single value.
    match = re.search(FIELD_RE_TEMPLATE.format(label="Quantity"), page, re.DOTALL)
    return clean_text(match.group(1)) if match else None


def extract_name(page: str, brands: Optional[list], quantity: Optional[str]) -> Optional[str]:
    """The heading is "<name> – <brand> – <quantity>" (the brand is left out
    when the name already contains it), so strip those suffixes."""
    match = NAME_RE.search(page)
    name = clean_text(match.group(1)) if match else None
    if not name:
        return None
    if quantity:
        name = strip_suffix(name, " – " + quantity)
    for brand in brands or []:
        stripped = strip_suffix(name, " – " + brand)
        if stripped != name:
            name = stripped
            break
    # Without a brand the heading still has its separator: "Pizza cheese – ".
    name = TRAILING_DASH_RE.sub("", name)
    # Products without a name show their barcode or only the quantity instead.
    if not name or name.replace(" ", "").isdigit() or name == quantity:
        return None
    return name


def extract_nutri_score(page: str) -> Optional[str]:
    match = NUTRISCORE_RE.search(page)
    return match.group(1).upper() if match else None


def extract_eco_score(page: str) -> Optional[str]:
    match = ECOSCORE_RE.search(page)
    if not match:
        return None
    return "A+" if match.group(1) == "a-plus" else match.group(1).upper()


def _ingredients_panel(page: str) -> str:
    match = INGREDIENTS_PANEL_RE.search(page)
    return match.group(1) if match else ""


def extract_ingredients(page: str) -> tuple:
    """Return (ingredients text, language of the text).

    The ingredients list is the panel text without a bold label, or with a
    language label ("French:") when it is not in English. The other labelled
    blocks are "Allergens:" and "Traces:".
    """
    for block in PANEL_TEXT_RE.findall(_ingredients_panel(page)):
        other_language = OTHER_LANGUAGE_RE.search(block)
        if other_language:
            language = other_language.group(1)
            return clean_text(other_language.group(2)), (None if language == "unknown language" else language)
        if "<strong" not in block:
            return clean_text(block), "English"
    return None, None


def extract_labelled_list(page: str, label: str) -> Optional[list]:
    """A '<strong>Allergens:</strong> milk, nuts' style list in the ingredients panel."""
    pattern = LABELLED_TEXT_RE_TEMPLATE.format(label=re.escape(label))
    match = re.search(pattern, _ingredients_panel(page), re.DOTALL)
    if not match:
        return None
    return dedupe([v.strip() for v in (clean_text(match.group(1)) or "").split(",") if v.strip()]) or None


def extract_allergens(page: str) -> Optional[list]:
    allergens = extract_labelled_list(page, "Allergens")
    if allergens:
        return allergens
    # Fallback: allergens highlighted inside the ingredients text.
    spans = [clean_text(s) for s in ALLERGEN_SPAN_RE.findall(_ingredients_panel(page))]
    return dedupe([s.lower() for s in spans if s]) or None


def extract_additives(page: str, ingredients: Optional[str]) -> Optional[list]:
    additives = ["E" + code[1:].lower() for code, _ in ADDITIVE_HEADING_RE.findall(page)]
    if not additives and ingredients:
        additives = ["E" + code.lower() for code in E_NUMBER_RE.findall(ingredients)]
    return dedupe(additives) or None


def extract_additive_names(page: str) -> Optional[list]:
    """Names from the additive headings, e.g. "E951 - Aspartame" -> "Aspartame"."""
    names = [clean_text(name) for _, name in ADDITIVE_HEADING_RE.findall(page)]
    return dedupe([n for n in names if n]) or None


def extract_nutrition(page: str) -> Optional[dict]:
    match = NUTRITION_TABLE_RE.search(page)
    if not match:
        return None
    table = match.group(1)

    # Use the "for 100 g / 100 ml" column; there may also be "per serving",
    # "prepared" or "compared to" columns. Column 0 holds the nutrient name.
    headers = [clean_text(h) or "" for h in HEADER_CELL_RE.findall(table)]
    col = next((i for i, h in enumerate(headers) if PER_100_RE.search(h)), 1)

    nutrition = {key: None for key in dict.fromkeys(NUTRIENT_ROWS.values())}
    for row in ROW_RE.findall(table):
        cells = CELL_RE.findall(row)
        if len(cells) <= col:
            continue  # header row or incomplete row
        key = NUTRIENT_ROWS.get((clean_text(cells[0]) or "").lower())
        if not key or nutrition[key] is not None:
            continue
        value = clean_text(cells[col]) or ""
        if key == "energy-kcal":
            kcal = KCAL_RE.search(value)
            nutrition[key] = float(kcal.group(1).replace(",", ".")) if kcal else None
        else:
            nutrition[key] = to_float(value)

    return nutrition if any(v is not None for v in nutrition.values()) else None


def extract_product(page: str, url: Optional[str] = None) -> dict:
    """Extract all fields from one product page. Missing fields are None."""
    url = url or extract_url(page)
    brands = extract_field(page, "Brands")
    quantity = extract_quantity(page)
    ingredients, ingredients_language = extract_ingredients(page)
    barcode = BARCODE_RE.search(url or "")
    return {
        "url": url,
        "barcode": barcode.group(1) if barcode else None,
        "product_name": extract_name(page, brands, quantity),
        "brands": brands,
        "quantity": quantity,
        "categories": extract_field(page, "Categories"),
        "labels": extract_field(page, "Labels, certifications, awards"),
        "countries": extract_field(page, "Countries where sold"),
        "nutri_score": extract_nutri_score(page),
        "eco_score": extract_eco_score(page),
        "ingredients_text": ingredients,
        "ingredients_language": ingredients_language if ingredients else None,
        "additives": extract_additives(page, ingredients),
        "additive_names": extract_additive_names(page),
        "allergens": extract_allergens(page),
        "traces": extract_labelled_list(page, "Traces"),
        "nutrition_100g": extract_nutrition(page),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def read_html(path: str) -> str:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return f.read()


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract product data from crawled HTML")
    parser.add_argument("--limit", type=int, default=None, help="only process the first N pages")
    parser.add_argument("--output", default=OUTPUT_FILE)
    args = parser.parse_args()

    paths = sorted(glob.glob(HTML_GLOB))[: args.limit]
    tmp_path = args.output + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as out:
        for i, path in enumerate(paths, 1):
            try:
                record = extract_product(read_html(path))
            except (OSError, UnicodeDecodeError) as exc:
                print(f"Skipping unreadable {path}: {exc}")
                continue
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            if i % 1000 == 0:
                print(f"{i}/{len(paths)} pages")
    os.replace(tmp_path, args.output)  # output is derived data, rebuilt each run
    print(f"Extracted {len(paths)} products -> {args.output}")


if __name__ == "__main__":
    main()
