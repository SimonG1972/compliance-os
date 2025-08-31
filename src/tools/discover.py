# src/tools/discover.py
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import List, Dict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from ..db import get_engine, begin
from ..log import info, warn

_POLICY_PAT = re.compile(
    r"(privacy|terms|legal|policy|policies|guidelines|community|cookie|cookies|ads|advertis(e|ing)|"
    r"safety|security|children|kid|minor|youth|trust|content\-policy)",
    re.I,
)

_DOCTYPE_MAP = [
    (re.compile(r"privacy|cookie", re.I), "privacy"),
    (re.compile(r"terms|tos|conditions", re.I), "terms"),
    (re.compile(r"guidelines|community|rules|moderation", re.I), "guidelines"),
    (re.compile(r"ads|advertis(e|ing)", re.I), "ads"),
    (re.compile(r"ip|copyright|dmca|trademark", re.I), "ip"),
    (re.compile(r"policy|policies|legal|safety|security|trust", re.I), "policy"),
    (re.compile(r"child|children|minor|youth|coppa", re.I), "children"),
]


def _guess_doc_type(url: str) -> str:
    for rx, dtype in _DOCTYPE_MAP:
        if rx.search(url):
            return dtype
    return "other"


def _fetch_html(url: str, timeout: float = 20.0) -> str:
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        }
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            ctype = (r.headers.get("content-type") or "").lower()
            if "text/html" in ctype or "application/xhtml" in ctype:
                return r.text or ""
    except Exception as e:
        warn(f"discover: fetch error {url} :: {e}")
    return ""


def _homepage_links(home: str, limit: int) -> List[str]:
    html = _fetch_html(home)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        absu = urljoin(home, href)
        if urlparse(absu).netloc != urlparse(home).netloc:
            continue
        if _POLICY_PAT.search(absu):
            links.append(absu)
        if len(links) >= limit:
            break
    return list(dict.fromkeys([home] + links))


def _sitemap_urls(home: str, limit: int) -> List[str]:
    base = f"{urlparse(home).scheme}://{urlparse(home).netloc}"
    for path in ("/sitemap.xml", "/sitemap_index.xml"):
        url = base + path
        try:
            r = requests.get(
                url,
                timeout=20.0,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0 Safari/537.36"
                    )
                },
            )
            if r.status_code != 200:
                continue
            xml = r.text
            urls: List[str] = []
            try:
                root = ET.fromstring(xml)
            except Exception:
                continue

            if root.tag.endswith("sitemapindex"):
                for sm in root.iter():
                    if sm.tag.endswith("loc") and sm.text:
                        loc = sm.text.strip()
                        try:
                            rs = requests.get(loc, timeout=20.0)
                            if rs.status_code == 200:
                                rroot = ET.fromstring(rs.text)
                                for u in rroot.iter():
                                    if u.tag.endswith("loc") and u.text:
                                        loc2 = u.text.strip()
                                        if urlparse(loc2).netloc == urlparse(home).netloc:
                                            if _POLICY_PAT.search(loc2):
                                                urls.append(loc2)
                                            if len(urls) >= limit:
                                                return list(dict.fromkeys(urls))
                        except Exception:
                            continue
                return list(dict.fromkeys(urls))
            elif root.tag.endswith("urlset"):
                for u in root.iter():
                    if u.tag.endswith("loc") and u.text:
                        loc = u.text.strip()
                        if urlparse(loc).netloc == urlparse(home).netloc:
                            if _POLICY_PAT.search(loc):
                                urls.append(loc)
                            if len(urls) >= limit:
                                break
                return list(dict.fromkeys(urls))
        except Exception as e:
            warn(f"discover: sitemap fetch error for {url} :: {e}")
    return []


def discover_from_homepage(homepage: str, limit: int = 50) -> List[Dict]:
    links = _homepage_links(homepage, limit=limit) or []
    sm = _sitemap_urls(homepage, limit=limit) or []
    merged = list(dict.fromkeys(links + sm))

    results: List[Dict] = []
    src = urlparse(homepage).netloc.replace("www.", "")
    for u in merged:
        results.append(
            {
                "url": u,
                "doc_type": _guess_doc_type(u),
                "jurisdiction": "global",
                "source": src,
            }
        )
    info(f"discover_from_homepage: {urlparse(homepage).netloc} (found={len(results)})")
    return results


def discover_by_query(query: str, limit: int = 50) -> List[Dict]:
    warn(f"discover_by_query: not implemented; query='{query}'")
    return []


def upsert_discovered(rows: List[Dict]) -> int:
    if not rows:
        return 0

    engine = get_engine()
    inserted = 0
    with begin() as conn:
        for r in rows:
            url = r.get("url")
            if not url:
                continue
            doc_type = r.get("doc_type") or "other"
            juris = r.get("jurisdiction") or "global"
            source = r.get("source") or urlparse(url).netloc.replace("www.", "")
            conn.execute(
                text(
                    """
                    INSERT OR IGNORE INTO documents (url, doc_type, jurisdiction, source)
                    VALUES (:url, :doc_type, :jurisdiction, :source)
                    """
                ),
                {"url": url, "doc_type": doc_type, "jurisdiction": juris, "source": source},
            )
            inserted += 1
    info(f"upsert_discovered: attempted={len(rows)}")
    return inserted
