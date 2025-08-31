# src/tools/hydrate.py
from __future__ import annotations
import hashlib
import os
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import requests
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from src.db import get_engine

# --- simple cleaner (kept local to avoid other imports) ---
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\u00A0]+")
def _clean_html_text(html: str) -> str:
    if not html:
        return ""
    txt = _TAG_RE.sub(" ", html)
    txt = re.sub(r"&nbsp;", " ", txt)
    txt = _WS_RE.sub(" ", txt)
    txt = re.sub(r"\s+\n", "\n", txt)
    txt = re.sub(r"\n\s+", "\n", txt)
    return txt.strip()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

@dataclass
class Candidate:
    url: str
    url_original: str
    title: str | None
    fetched_at: str | None
    content_hash: str | None
    retry_count: int | None
    last_error: str | None

# ---- selection helpers -------------------------------------------------------

def _select_candidates(conn: Connection, limit: int, contains: Optional[str], changed_only: bool) -> list[Candidate]:
    """
    Pull a batch of URLs to hydrate. We *do not* open a surrounding transaction
    here—each row will be handled with its own autocommit write.
    """
    where_parts = []
    params = {}

    # Empty or never-fetched
    where_parts.append("(body IS NULL OR body = '')")

    if changed_only:
        # Anything older than 3 days or no hash/timestamp yet
        params["cutoff"] = (_now_iso()[:10],)  # date prefix ok; we also allow fresh
        where_parts.append("(content_hash IS NULL OR fetched_at IS NULL)")

    if contains:
        params["like1"] = f"%{contains}%"
        params["like2"] = f"%{contains}%"
        where_parts.append("(url LIKE :like1 OR title LIKE :like2)")

    where_sql = " OR ".join(where_parts) if where_parts else "1=1"
    sql = f"""
        SELECT url, COALESCE(url_original,'') AS url_original, title, fetched_at, content_hash, retry_count, last_error
        FROM documents
        WHERE {where_sql}
        LIMIT :lim
    """
    params["lim"] = limit
    rows = conn.execute(text(sql), params).fetchall()
    out: list[Candidate] = []
    for r in rows:
        out.append(Candidate(
            url=r.url,
            url_original=r.url_original or r.url,
            title=getattr(r, "title", None),
            fetched_at=getattr(r, "fetched_at", None),
            content_hash=getattr(r, "content_hash", None),
            retry_count=getattr(r, "retry_count", 0) or 0,
            last_error=getattr(r, "last_error", ""),
        ))
    return out

# ---- network -----------------------------------------------------------------

def _http_get(url: str, timeout: float = 25.0) -> tuple[int, str]:
    """
    Basic static GET. No headless browser here; that lives elsewhere.
    """
    try:
        resp = requests.get(url, timeout=timeout, headers={
            "User-Agent": "compliance-os/1.0 (+bot; hydration)"
        })
        return resp.status_code, resp.text if isinstance(resp.text, str) else ""
    except Exception as e:
        return 0, f"REQUEST_ERROR: {e!r}"

# ---- DB writes (per-row autocommit!) -----------------------------------------

_UPSERT_SQL = text("""
    INSERT INTO documents (url, url_original, title, body, clean_text,
                           status_code, render_mode, fetched_at,
                           content_hash, revisions, last_error, retry_count)
    VALUES (:url, :url_original, COALESCE(:title,''), :body, :clean_text,
            :status_code, :render_mode, :fetched_at,
            :content_hash, 0, :last_error, :retry_count)
    ON CONFLICT(url) DO UPDATE SET
        url_original=excluded.url_original,
        title=COALESCE(excluded.title, documents.title),
        body=excluded.body,
        clean_text=excluded.clean_text,
        status_code=excluded.status_code,
        render_mode=excluded.render_mode,
        fetched_at=excluded.fetched_at,
        revisions=CASE
            WHEN documents.content_hash IS NOT excluded.content_hash
                 AND documents.content_hash IS NOT NULL
                 AND documents.content_hash != excluded.content_hash
                 THEN documents.revisions + 1
            ELSE documents.revisions
        END,
        content_hash=excluded.content_hash,
        last_error=excluded.last_error,
        retry_count=excluded.retry_count
""")

def _upsert_one(eng: Engine, payload: dict) -> None:
    """
    Per-document autocommit write. Any error affects only this row.
    """
    with eng.begin() as conn:
        conn.execute(_UPSERT_SQL, payload)

# ---- public entrypoint --------------------------------------------------------

def hydrate(*,
            limit: int = 100,
            pause: float = 0.25,
            contains: Optional[str] = None,
            changed_only: bool = False,
            urls: Optional[Iterable[str]] = None) -> None:
    """
    Hydrate documents:
      - If `urls` provided → hydrate only those URLs.
      - Else → pick candidates from DB (by contains/changed_only/limit).
    Each document is fetched and **committed individually** so one failure
    does not erase progress.
    """
    eng = get_engine()

    if urls:
        todo = [Candidate(url=u, url_original=u, title=None, fetched_at=None, content_hash=None, retry_count=0, last_error="") for u in urls]
    else:
        with eng.connect() as conn:
            todo = _select_candidates(conn, limit=limit, contains=contains, changed_only=changed_only)

    if not todo:
        print("[hydrate] nothing to do")
        return

    ok = 0
    fail = 0
    started = time.time()

    for i, c in enumerate(todo, 1):
        print(f"[http] -> get {c.url}")
        status_code, body_or_err = _http_get(c.url)
        fetched_at = _now_iso()

        if status_code != 200:
            # Record error row (commit anyway so dashboard shows it)
            payload = {
                "url": c.url,
                "url_original": c.url_original or c.url,
                "title": c.title or "",
                "body": "",
                "clean_text": "",
                "status_code": status_code,
                "render_mode": "static",
                "fetched_at": fetched_at,
                "content_hash": None,
                "last_error": body_or_err[:500] if body_or_err else f"HTTP_{status_code}",
                "retry_count": (c.retry_count or 0) + 1,
            }
            try:
                _upsert_one(eng, payload)
                fail += 1
                print(f"[http] <- {status_code} {c.url} (recorded error)")
            except Exception:
                fail += 1
                print(f"[db] write error for {c.url}\n{traceback.format_exc()}")
            time.sleep(pause)
            continue

        # success path
        html = body_or_err or ""
        clean = _clean_html_text(html)
        content_hash = hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest() if html else None
        payload = {
            "url": c.url,
            "url_original": c.url_original or c.url,
            "title": c.title or "",
            "body": html,
            "clean_text": clean,
            "status_code": status_code,
            "render_mode": "static",
            "fetched_at": fetched_at,
            "content_hash": content_hash,
            "last_error": "",
            "retry_count": 0,
        }
        try:
            _upsert_one(eng, payload)
            ok += 1
            print(f"[http] <- 200 {c.url} (saved)")
        except Exception:
            fail += 1
            print(f"[db] write error for {c.url}\n{traceback.format_exc()}")

        time.sleep(pause)

    dur = time.time() - started
    print(f"[hydrate] done: ok={ok} fail={fail} in {dur:.1f}s")
