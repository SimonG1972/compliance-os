from bs4 import BeautifulSoup
from pathlib import Path

def html_to_sections(raw_html_path: str):
    """
    Parse a saved HTML snapshot into a list of sections:
    [
      {'title': '(intro)'|str, 'text': str, 'anchors': [str|empty]}
    ]
    Uses the built-in 'html.parser' to avoid native build deps.
    """
    html = Path(raw_html_path).read_text(errors="ignore")
    soup = BeautifulSoup(html, "html.parser")  # <- switched from 'lxml'
    # Remove noise
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    sections = []
    current = {"title": "(intro)", "text": [], "anchors": []}

    # Heuristic: treat headings as new sections, aggregate paragraphs/lis under them
    for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li"]):
        name = el.name.lower()
        if name.startswith("h"):
            # flush prior
            if current["text"]:
                current["text"] = "\n".join(current["text"])
                sections.append(current)
            # start new
            title = el.get_text(" ", strip=True)
            anchor = el.get("id") or ""
            current = {"title": title or "(untitled)", "text": [], "anchors": [anchor] if anchor else []}
        else:
            t = el.get_text(" ", strip=True)
            if t:
                current["text"].append(t)

    # flush the last section
    if current["text"]:
        current["text"] = "\n".join(current["text"])
        sections.append(current)

    # If nothing parsed, return a single blob
    if not sections:
        body_text = soup.get_text("\n", strip=True)
        if body_text:
            sections = [{"title": "(document)", "text": body_text, "anchors": []}]

    return sections
