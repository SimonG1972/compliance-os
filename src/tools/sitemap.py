# src/tools/sitemap.py
from __future__ import annotations
import os
import re
import time
import hashlib
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

CACHE_DIR = os.path.join(".cache", "sitemaps")
os.makedirs(CACHE_DIR, exist_ok=True)

# stricter match; we’ll also validate depth in code
POLICY_RE = re.compile(r"(privacy|terms|policy|legal|guidelines)", re.I)

def _cache_path(url: str) -> str:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{h}.xml")

def _fetch(url: str, ua="compliance-os-bot/0.1") -> str:
    path = _cache_path(url)
    if os.path.exists(path) and (time.time() - os.path.getmtime(path)) < 86400:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception:
            pass
    try:
        resp = requests.get(url, headers={"User-Agent": ua}, timeout=15)
        if resp.status_code == 200 and resp.text.strip():
            with open(path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            return resp.text
    except Exception:
        return ""
    return ""

def _robots_allowed(base_url: str, path: str, ua="compliance-os-bot/0.1") -> bool:
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(ua, path)
    except Exception:
        return True

def _looks_like_policy(u: str, origin_netloc: str) -> bool:
    """stricter: require keyword + shallow depth OR exact slugs."""
    parsed = urlparse(u)
    path = parsed.path.lower()
    if not POLICY_RE.search(path):
        return False

    # depth check (avoid very deep marketing/blog pages)
    depth = path.strip("/").count("/")
    if depth <= 3:
        # basic downranks
        if any(seg in path for seg in ("/help", "/blog", "/press", "/news", "/about")):
            return False
        return True

    # known exact slugs (allow even if depth > 3)
    exact = (
        "privacy-policy",
        "terms-of-service",
        "terms_and_conditions",
        "community-guidelines",
        "legal/privacy",
        "legal/terms",
    )
    return any(x in path for x in exact)

def _parse_sitemap(xml: str) -> list[str]:
    try:
        root = ET.fromstring(xml)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        return [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc is not None and loc.text]
    except Exception:
        return []

def discover_sitemap_links(base_url: str, per_run_cap: int = 1000, ua="compliance-os-bot/0.1") -> list[str]:
    """
    Return up to `per_run_cap` URLs from the site's sitemap(s) that are likely policy/terms/privacy/legal.
    Respects robots.txt on each candidate path.
    """
    base = f"{base_url.rstrip('/')}/sitemap.xml"
    origin_netloc = urlparse(base_url).netloc
    xml = _fetch(base, ua=ua)
    if not xml:
        return []

    urls = []
    try:
        root = ET.fromstring(xml)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        if root.tag.endswith("sitemapindex"):
            for sm in root.findall("sm:sitemap", ns):
                loc = sm.find("sm:loc", ns)
                if loc is not None and loc.text:
                    child_xml = _fetch(loc.text.strip(), ua=ua)
                    if child_xml:
                        urls.extend(_parse_sitemap(child_xml))
        else:
            urls.extend(_parse_sitemap(xml))
    except Exception:
        return []

    good: list[str] = []
    for u in urls:
        if len(good) >= per_run_cap:
            break
        if not _looks_like_policy(u, origin_netloc):
            continue
        p = urlparse(u).path
        if _robots_allowed(base_url, p, ua=ua):
            good.append(u)

    # dedupe while preserving order
    seen = set()
    out = []
    for u in good:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out
