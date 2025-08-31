"""
Content extraction helper.

Turns raw HTML into `clean_text` for AI/embeddings:
- keep <title>, <h1>-<h3>, <p>
- strip nav/footer/boilerplate/scripts/styles
- join as readable plain text
"""

from bs4 import BeautifulSoup

def extract_clean_text(html: str) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")

        # kill scripts, styles, nav, footer
        for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
            tag.decompose()

        chunks = []

        if soup.title and soup.title.string:
            chunks.append(soup.title.string.strip())

        for tag in soup.find_all(["h1", "h2", "h3", "p"]):
            txt = tag.get_text(" ", strip=True)
            if txt:
                chunks.append(txt)

        return "\n".join(chunks).strip()
    except Exception:
        return ""
