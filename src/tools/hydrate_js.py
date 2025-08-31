# src/tools/hydrate_js.py
from __future__ import annotations

import time
from typing import List, Dict

import requests
from sqlalchemy import text

from ..db import get_engine, begin
from ..log import info, warn

# Try Playwright for true JS render; otherwise use requests fallback.
try:
    from playwright.sync_api import sync_playwright
    _HAS_PW = True
except Exception:
    _HAS_PW = False


def _requests_fallback(url: str, timeout: float = 20.0) -> str:
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        }
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        ctype = (r.headers.get("content-type") or "").lower()
        if "text/html" in ctype or "application/xhtml" in ctype:
            return r.text or ""
    except Exception as e:
        warn(f"hydrate-js: requests fallback failed for {url} :: {e}")
    return ""


def render_single(url: str, timeout_ms: int = 45000) -> str:
    """
    Render a single URL with JS (Playwright), return HTML string.
    Falls back to simple requests if Playwright not available.
    """
    if not _HAS_PW:
        return _requests_fallback(url, timeout=timeout_ms / 1000.0)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(1200)
            html = page.content()
            context.close()
            browser.close()
            return html or ""
    except Exception as e:
        warn(f"hydrate-js: render_single failed for {url} :: {e}")
        return ""


def _select_domain_candidates(conn, domain: str, limit: int, only_empty: bool) -> List[Dict]:
    where = ["url LIKE :dom"]
    params = {"dom": f"%://%{domain}%", "limit": limit}
    if only_empty:
        where.append("(body IS NULL OR body = '')")
    sql = f"""
        SELECT url
          FROM documents
         WHERE {' AND '.join(where)}
         ORDER BY url
         LIMIT :limit
    """
    rows = conn.execute(text(sql), params).fetchall()
    return [{"url": r[0]} for r in rows]


def hydrate_js(
    domain: str,
    limit: int = 25,
    timeout_ms: int = 45000,
    only_empty: bool = True,
    pause: float = 0.25,
) -> int:
    """
    JS hydrate many URLs for a given domain (used by your CLI).
    """
    engine = get_engine()
    updated = 0
    with begin() as conn:
        candidates = _select_domain_candidates(conn, domain, limit, only_empty)

    if not candidates:
        info(f"hydrate-js: no candidates for domain={domain}")
        return 0

    for i, row in enumerate(candidates, 1):
        url = row["url"]
        info(f"hydrate-js: [{i}/{len(candidates)}] {url}")
        html = render_single(url, timeout_ms=timeout_ms)
        if html and html.strip():
            with begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE documents
                           SET body = :body,
                               render_mode = 'js',
                               status_code = COALESCE(status_code, 200),
                               fetched_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
                         WHERE url = :url
                        """
                    ),
                    {"body": html, "url": url},
                )
            updated += 1
        if pause:
            time.sleep(pause)

    info(f"hydrate-js done; updated={updated}")
    return updated
