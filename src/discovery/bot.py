# src/discovery/bot.py
from __future__ import annotations

import re
import time
from typing import Dict, List, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

from ..db import begin
from ..log import info, warn
from sqlalchemy import text

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"}
HTTP_TIMEOUT = 12.0

KEYWORDS = [
    "privacy", "terms", "community", "policy", "policies", "legal",
    "guidelines", "safety", "advertising", "ads", "ip", "copyright", "dmca",
    "children", "minors", "trust", "moderation", "transparency", "gdpr",
]

# Common synthetic endpoints to try on known domains
SYNTH_PATHS = [
    "/legal/privacy", "/privacy", "/privacy-policy",
    "/legal/terms", "/terms", "/terms-of-service", "/tos",
    "/policies", "/policy", "/community-guidelines", "/rules",
    "/legal", "/help/center", "/safety", "/about/policies",
]

DOC_TYPE_RULES: List[Tuple[str, str]] = [
    (r"privacy", "privacy"),
    (r"terms|tos", "terms"),
    (r"guidelines|community|rules", "guidelines"),
    (r"ads|advertis", "ads"),
    (r"policy|policies|legal", "policy"),
    (r"children|minors|youth|kid", "children"),
    (r"copyright|dmca|ip", "ip"),
]


def _normalize_url(url: str) -> str:
    try:
        p = urlparse(url.strip())
        scheme = "https"
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = p.path or "/"
        return f"{scheme}://{host}{path}".rstrip("/")
    except Exception:
        return url


def _infer_doc_type(url_or_title: str) -> str:
    s = url_or_title.lower()
    for pat, label in DOC_TYPE_RULES:
        if re.search(pat, s):
            return label
    return "other"


def _infer_platform(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    # crude extraction: site name up to TLD
    base = host.split(":")[0]
    parts = base.split(".")
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return base.capitalize() or "Unknown"


def _infer_jurisdiction(url: str) -> str:
    # If URL path contains locale or region hints, guess; else global
    path = urlparse(url).path.lower()
    if "/eu" in path or "/en-eu" in path or "/europe" in path:
        return "eu"
    if "/uk" in path or "/en-gb" in path:
        return "uk"
    if "/us" in path or "/en-us" in path:
        return "us"
    return "global"


def _fetch(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if 200 <= r.status_code < 400:
            return r.text or ""
        return ""
    except Exception:
        return ""


def _extract_candidate_links(homepage: str, html: str, limit: int) -> List[str]:
    out: List[str] = []
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return out

    # Collect all <a href> with keyword hits
    hrefs: List[str] = []
    for a in soup.find_all("a", href=True):
        hrefs.append(urljoin(homepage, a["href"]))

    # Filter by keywords
    homepage_host = urlparse(homepage).netloc
    for u in hrefs:
        if not u.startswith("http"):
            continue
        # keep same-site primarily (loose check)
        if urlparse(u).netloc and urlparse(u).netloc.split(":")[0] not in homepage_host:
            # allow some cross-subdomain links (e.g., policies.tiktok.com)
            pass
        low = u.lower()
        if any(k in low for k in KEYWORDS):
            out.append(u)

    # Add synthetic well-known paths
    base = _normalize_url(homepage)
    for p in SYNTH_PATHS:
        out.append(urljoin(base + "/", p.lstrip("/")))

    # De-dupe while preserving order
    seen = set()
    deduped = []
    for u in out:
        nu = _normalize_url(u)
        if nu not in seen:
            seen.add(nu)
            deduped.append(nu)

    return deduped[: max(1, int(limit))]


# ---------------------------
# Public API
# ---------------------------

def discover_from_homepage(homepage: str, limit: int = 50) -> List[Dict[str, str]]:
    """
    Fetch homepage, extract policy-like links and synthesize common endpoints.
    Returns a list of dicts suitable for upsert_discovered().
    """
    homepage = _normalize_url(homepage)
    info(f"discover_from_homepage: {homepage} (limit={limit})")
    html = _fetch(homepage)
    if not html:
        warn(f"discover_from_homepage: empty html for {homepage}")

    candidates = _extract_candidate_links(homepage, html, limit=limit)
    out: List[Dict[str, str]] = []
    for u in candidates:
        out.append({
            "url": u,
            "doc_type": _infer_doc_type(u),
            "platform_or_regulator": _infer_platform(u),
            "jurisdiction": _infer_jurisdiction(u),
            "title": "",      # will be hydrated later
            "body": "",       # will be hydrated later
            "homepage": homepage,
        })
    info(f"discover_from_homepage: found {len(out)} candidates")
    return out


def discover_by_query(query: str, limit: int = 25) -> List[Dict[str, str]]:
    """
    Lightweight heuristic:
    - If query looks like a brand, generate common compliance endpoints on its domain.
    - Otherwise, just return empty (no web search dependency here).
    """
    q = (query or "").strip()
    if not q:
        return []

    # crude brand → domain mapping heuristic
    brand = q.split()[0].lower()
    domain_guess = {
        "tiktok": "https://www.tiktok.com",
        "youtube": "https://www.youtube.com",
        "instagram": "https://www.instagram.com",
        "facebook": "https://www.facebook.com",
        "etsy": "https://www.etsy.com",
        "snapchat": "https://www.snapchat.com",
        "reddit": "https://www.reddit.com",
        "pinterest": "https://www.pinterest.com",
        "ftc": "https://www.ftc.gov",
        "amazon": "https://www.amazon.com",
        "ebay": "https://www.ebay.com",
        "x": "https://x.com",
        "twitter": "https://x.com",
    }.get(brand)

    if not domain_guess:
        return []

    return discover_from_homepage(domain_guess, limit=limit)


def upsert_discovered(cands: List[Dict[str, str]]) -> int:
    """
    Insert or update rows into the base `documents` table.
    Returns count of upserted rows.
    """
    if not cands:
        return 0

    upsert_sql = text("""
        INSERT INTO documents (url, doc_type, platform_or_regulator, jurisdiction, title, body, homepage, discovered_at)
        VALUES (:url, :doc_type, :platform_or_regulator, :jurisdiction, :title, :body, :homepage, strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        ON CONFLICT(url) DO UPDATE SET
            doc_type=excluded.doc_type,
            platform_or_regulator=excluded.platform_or_regulator,
            jurisdiction=excluded.jurisdiction,
            homepage=excluded.homepage
    """)

    n = 0
    with begin() as conn:
        for row in cands:
            try:
                conn.execute(upsert_sql, row)
                n += 1
            except Exception as e:
                warn(f"upsert failed for {row.get('url','')}: {e!r}")
    info(f"upsert_discovered: {n} rows")
    return n
