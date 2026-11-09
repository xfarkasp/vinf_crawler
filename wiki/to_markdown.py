"""Convert a DokuWiki page (wiki/*.txt) to Markdown, to preview what goes on the wiki.

Usage: python wiki/to_markdown.py wiki/konzultacia_3.txt     # writes wiki/konzultacia_3.md
"""
import re
import sys

IMAGE_DIR = "../diagram/"  # where uploaded images live locally, relative to wiki/


def convert_inline(text: str) -> str:
    protected = []

    def protect(value: str) -> str:
        protected.append(value)
        return f"\x00{len(protected) - 1}\x00"

    # Inline code: ''%%text%%'' and ''text''
    text = re.sub(r"''%%(.*?)%%''", lambda m: protect(f"`{m.group(1)}`"), text)
    text = re.sub(r"''(.*?)''", lambda m: protect(f"`{m.group(1)}`"), text)

    def media(m):  # {{name.png?900|alt}} or {{file.zip|text}}
        target, _, label = m.group(1).partition("|")
        target = target.split("?")[0].lstrip(":")
        if target.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".svg")):
            return protect(f"![{label or target}]({IMAGE_DIR}{target.split(':')[-1]})")
        return protect(f"[{label or target}]({target})")
    text = re.sub(r"\{\{(.*?)\}\}", media, text)

    def link(m):  # [[url|text]] and [[url]]
        target, _, label = m.group(1).partition("|")
        return protect(f"[{label}]({target})" if label else f"<{target}>")
    text = re.sub(r"\[\[(.*?)\]\]", link, text)

    text = re.sub(r"(?<!:)//(.+?)//", r"*\1*", text)  # italics (URLs are protected)
    return re.sub(r"\x00(\d+)\x00", lambda m: protected[int(m.group(1))], text)


def split_cells(line: str) -> list:
    """(text, opening separator) per cell; separators inside links/code don't count."""
    cells, current, opener, depth, in_code, i = [], "", None, 0, False, 0
    while i < len(line):
        if line.startswith("[[", i) or line.startswith("]]", i) or line.startswith("''", i):
            token = line[i:i + 2]
            depth += {"[[": 1, "]]": -1}.get(token, 0)
            in_code ^= token == "''"
            current += token
            i += 2
            continue
        ch = line[i]
        if ch in "|^" and depth == 0 and not in_code:
            if opener is not None:
                cells.append((current, opener))
            current, opener = "", ch
        else:
            current += ch
        i += 1
    return cells


def convert(source: str) -> str:
    lines, out, i = source.split("\n"), [], 0
    while i < len(lines):
        line = lines[i]
        code = re.match(r"<code\s*(\w*)>", line.strip())
        if code:
            out.append(f"```{code.group(1)}")
            i += 1
            while i < len(lines) and lines[i].strip() != "</code>":
                out.append(lines[i])
                i += 1
            out.append("```")
            i += 1
            continue
        heading = re.match(r"^(={2,6})\s*(.*?)\s*\1\s*$", line)
        if heading:
            out.append("#" * (7 - len(heading.group(1))) + " " + convert_inline(heading.group(2)))
            i += 1
            continue
        if line.startswith(("^", "|")):
            first = True
            while i < len(lines) and lines[i].startswith(("^", "|")):
                cells = split_cells(lines[i].rstrip())
                header = first and lines[i].startswith("^")
                values = []
                for text, sep in cells:
                    value = convert_inline(text.strip()) or " "
                    # Markdown tables split on every "|", even inside `code`: escape it there.
                    value = re.sub(r"`[^`]*`", lambda m: m.group(0).replace("|", "\\|"), value)
                    values.append(f"**{value}**" if sep == "^" and not header and value.strip() else value)
                row = "| " + " | ".join(values) + " |"
                if first and not header:
                    out += ["|" + " |" * len(values), "|" + "---|" * len(values)]
                out.append(row)
                if header:
                    out.append("|" + "---|" * len(values))
                first = False
                i += 1
            continue
        item = re.match(r"^(\s{2,})([*-])\s+(.*)$", line)
        if item:
            indent = "  " * (len(item.group(1)) // 2 - 1)
            out.append(f"{indent}{'-' if item.group(2) == '*' else '1.'} {convert_inline(item.group(3))}")
            i += 1
            continue
        out.append(convert_inline(line))
        i += 1
    return "\n".join(out)


if __name__ == "__main__":
    source = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else re.sub(r"\.txt$", ".md", source)
    with open(source, encoding="utf-8") as f:
        markdown = convert(f.read())
    with open(target, "w", encoding="utf-8") as f:
        f.write(markdown)
    print(f"Wrote {target}")
