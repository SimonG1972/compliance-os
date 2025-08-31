# src/fetchers/http_fetcher.py
from __future__ import annotations
import os
import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

# --- env-driven knobs (safe defaults) ---
UA = os.getenv("CO_UA", "compliance-os-bot/0.1")
HTTP_TIMEOUT = float(os.getenv("CO_HTTP_TIMEOUT", "12"))           # seconds
HOST_DELAY   = float(os.getenv("CO_HOST_DELAY", "1.2"))            # seconds between same-host requests
MAX_RETRIES  = int(os.getenv("CO_MAX_RETRIES", "3"))               # per call
BACKOFF_BASE = float(os.getenv("CO_BACKOFF_BASE", "2"))            # 2, 1.5, etc.

_last_request_time: dict[str, float] = {}
_robots_cache: dict[str, RobotFileParser | None] = {}


def _allowed_by_robots(url: str) -> bool:
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    rp = _robots_cache.get(base)
    if rp is None:
        rp = RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
        except Exception:
            _robots_cache[base] = None
            return True
        _robots_cache[base] = rp
    elif rp is False:  # never set, keep permissive
        return True
    return _robots_cache[base].can_fetch(UA, url) if _robots_cache[base] else True


def _host_throttle(host: str):
    last = _last_request_time.get(host)
    if last:
        wait = HOST_DELAY - (time.time() - last)
        if wait > 0:
            time.sleep(wait)


def fetch_html(url: str, max_retries: int | None = None):
    """
    Fetch URL with polite headers, retry/backoff, host throttling.
    Returns (html, status_code, render_mode, error)
    """
    if not _allowed_by_robots(url):
        return None, None, None, "robots"

    parsed = urlparse(url)
    host = parsed.netloc
    retries = 0
    max_r = max_retries if max_retries is not None else MAX_RETRIES
    delay = 1.0

    while retries <= max_r:
        _host_throttle(host)
        try:
            resp = requests.get(url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
            _last_request_time[host] = time.time()
            sc = resp.status_code

            if sc == 200 and resp.text.strip():
                return resp.text, 200, "static", None

            # polite backoff for 403/429
            if sc in (403, 429):
                time.sleep(delay)
                delay *= BACKOFF_BASE
                retries += 1
                continue

            # other non-200s: return error without retry storm
            return None, sc, "static", f"HTTP {sc}"
        except Exception as e:
            retries += 1
            time.sleep(delay)
            delay *= BACKOFF_BASE
            if retries > max_r:
                return None, None, None, str(e)

    return None, None, None, "max_retries"
