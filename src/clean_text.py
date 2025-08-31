import re, html
from html.parser import HTMLParser

# Lightweight tag stripper (fast)
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript|template)[\s\S]*?</\1>", re.I)
JSON_BLOB_RE = re.compile(r"\{[^{}]{200,}\}")  # suppress giant inline JSON
MENU_HINTS = re.compile(r"(navigation|menu|footer|breadcrumbs|cookie\s*banner|subscribe|share)", re.I)

# Sentence splitter (period, question, exclamation + unicode)
SENT_SPLIT = re.compile(r"(?<=[\.\?\!])\s+(?=[A-Z0-9“\"'])")

WS_RE = re.compile(r"\s+")

def strip_html_keep_text(html_text: str) -> str:
    if not html_text:
        return ""
    # remove scripts/styles early
    t = SCRIPT_STYLE_RE.sub(" ", html_text)
    # drop obviously huge json/config blobs
    t = JSON_BLOB_RE.sub(" ", t)
    # strip tags
    t = TAG_RE.sub(" ", t)
    t = html.unescape(t)
    t = WS_RE.sub(" ", t).strip()
    return t

def is_menuish(sent: str) -> bool:
    # filter out likely nav/boilerplate sentences
    if len(sent) < 30:  # very short UI fragments
        return True
    if MENU_HINTS.search(sent):
        return True
    # suppress extreme symbol/letter imbalance
    letters = sum(c.isalpha() for c in sent)
    non = len(sent) - letters
    return letters < 0.4 * non

def to_sentences(clean_text: str) -> list[str]:
    if not clean_text:
        return []
    sents = SENT_SPLIT.split(clean_text)
    out = []
    for s in sents:
        s = s.strip()
        if not s:
            continue
        if is_menuish(s):
            continue
        out.append(s)
    return out

def signal_score(sent: str) -> float:
    # reward letters, digits, commas/periods; penalize braces/JSy chars
    if not sent: return 0.0
    letters = sum(c.isalnum() for c in sent)
    good_punct = sum(c in ".,;:?!’'“”\"()" for c in sent)
    bad = sum(c in "{}[]<>=+/*$#_|\\`~" for c in sent)
    return (letters + 0.5*good_punct) / (1 + bad + abs(len(sent) - 180) * 0.005)
