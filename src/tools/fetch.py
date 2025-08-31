# src/tools/fetch.py
from __future__ import annotations

import hashlib
import re
import time
from typing import Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _UA, "Accept": "text/html,application/xhtml+xml"}
_STRIP_RE = re.compile(r"\s+")


def _clean_text(txt: str) -> str:
    return _STRIP_RE.sub(" ", (txt or "").strip())


def sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def fetch_page(url: str, timeout: float = 12.0, pause: float = 0.0) -> Tuple[str, str, int, str]:
    """
    Fetch a URL and return (title, text, http_status, final_url).
    """
    try:
        if pause:
            time.sleep(pause)

        resp = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        http_status = resp.status_code
        final_url = resp.url

        if not (200 <= http_status < 300):
            return "", "", http_status, final_url

        html = resp.text or ""
        soup = BeautifulSoup(html, "lxml")

        # Title preference: <title> then og:title
        title = ""
        if soup.title and soup.title.string:
            title = _clean_text(soup.title.string)
        if not title:
            og = soup.find("meta", attrs={"property": "og:title"})
            if og and og.get("content"):
                title = _clean_text(og["content"])

        # Body: main/article or largest block
        candidates = []
        main = soup.find("main")
        if main:
            candidates.append(main)
        article = soup.find("article")
        if article:
            candidates.append(article)
        candidates.extend(soup.find_all(["section", "div"], recursive=True)[:50])

        best_text = ""
        best_len = 0
        for node in candidates:
            txt = _clean_text(node.get_text(separator=" "))
            ln = len(txt)
            if ln > best_len:
                best_text = txt
                best_len = ln

        text = best_text or _clean_text(soup.get_text(separator=" "))
        return (title or "", text or "", http_status, final_url)
    except Exception:
        return "", "", 0, url
