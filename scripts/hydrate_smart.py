#!/usr/bin/env python
# scripts/hydrate_smart.py
#
# Smart hydration:
#  - Skips obvious assets + tracker redirectors
#  - Uses ETag/Last-Modified for light re-fetch
#  - Falls back to Playwright render for thin/blocked HTML
#  - Marks errors/rate limits but leaves row for retry
#  - NEW: Updates discovery_queue.status and prints live + final progress

import os, re, time, sqlite3, argparse, hashlib
import urllib.request, urllib.error
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Optional

# === BEGIN POLICY IMPORTS (safe/no-op if missing) ===
try:
    from src.policy.loader import get_policy_for_url, policy_enforce_enabled
    from src.tools.normalize_policy import suggest_normalized_url
    _POLICY_AVAILABLE = True
except Exception:
    _POLICY_AVAILABLE = False
# === END POLICY IMPORTS ===

DB = os.path.join(os.getcwd(), "compliance.db")
UA = os.environ.get("COMPLIANCE_UA", "ComplianceOS-Fetch/3.0")
PROXY = os.environ.get("COMPLIANCE_HTTP_PROXY") or None

# --- URL filters --------------------------------------------------------------

_ASSET_EXT = re.compile(
    r"\.(?:png|jpe?g|gif|webp|avif|svg|ico|css|js|mjs|woff2?|ttf|otf|mp4|webm|mov|m4v|mp3|wav|ogg|zip|7z|rar|tar|gz)$",
    re.I,
)
_GITBOOK_IMG = re.compile(r"/~gitbook/image", re.I)
_QUERY_ASSET_HINT = re.compile(r"(?:\bwidth=|\bdpr=|\bquality=|\bformat=)", re.I)
_SKIP_PATH_HINTS = re.compile(r"/(?:static|assets|img|images|media|embed)/", re.I)
# tracker/redirect patterns (e.g., FB privacy_center /l/?logging_data=*)
_SKIP_TRACKERS = re.compile(r"/l/\?logging_data|[?&](?:utm_[a-z]+|fbclid|gclid)=", re.I)

def is_probably_asset(u: str, allow_pdf=True) -> bool:
    try:
        p = urlparse(u)
    except Exception:
        return True
    path = p.path or ""
    q = p.query or ""
    # PDFs are allowed (lots of legal docs are PDFs)
    if allow_pdf and path.lower().endswith(".pdf"):
        return False
    # Skip obvious tracker/redirector URLs
    if _SKIP_TRACKERS.search(u):
        return True
    # Obvious assets
    if _ASSET_EXT.search(path): return True
    if _GITBOOK_IMG.search(path): return True
    if _QUERY_ASSET_HINT.search(q): return True
    if _SKIP_PATH_HINTS.search(path): return True
    return False

# --- HTTP client --------------------------------------------------------------

def _opener():
    if PROXY:
        return urllib.request.build_opener(urllib.request.ProxyHandler({"http": PROXY, "https": PROXY}))
    return urllib.request.build_opener()

def http_get(url, etag=None, last_mod=None, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    if etag: req.add_header("If-None-Match", etag)
    if last_mod: req.add_header("If-Modified-Since", last_mod)
    op = _opener()
    try:
        with op.open(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", "ignore")
            code = r.getcode()
            headers = dict(r.headers.items())
            et = headers.get("ETag")
            lm = headers.get("Last-Modified")
            ct = headers.get("Content-Type", "") or ""
            return code, body, et, lm, ct
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", "ignore")
        except Exception:
            err_body = ""
        return e.code, err_body, None, None, ""
    except Exception:
        return 0, "", None, None, ""

# --- Render fallback (Playwright) --------------------------------------------

def try_render(url, timeout_ms=25000, wait_after_ms=1200, headed=False):
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return "", "playwright-not-installed"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=not headed)
            ctx = browser.new_context(user_agent=UA)
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(wait_after_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:
                pass
            html = page.content()
            browser.close()
            return html, "render-ok"
    except Exception:
        return "", "render-error"

def should_render(code, body, ctype, render_on, min_html_len):
    if code in render_on:
        return True
    if (not ctype) or ("text/html" in (ctype or "").lower()):
        if len(body or "") < min_html_len:
            return True
    return False

# --- Policy helpers (local) ---------------------------------------------------

def _is_thin_html(text: str, min_chars: Optional[int]) -> bool:
    if not min_chars:
        return False
    return len(text or "") < int(min_chars)

def _script_ratio(html: str) -> float:
    if not html:
        return 0.0
    total_tags = html.count("<")
    if total_tags <= 0:
        return 0.0
    return html.lower().count("<script") / total_tags

# --- Helpers: progress + queue status ----------------------------------------

def db_counts(cur):
    tot = cur.execute("SELECT COUNT(*) FROM discovery_queue").fetchone()[0]
    hyd = cur.execute("SELECT COUNT(*) FROM discovery_queue WHERE status = 'hydrated'").fetchone()[0]
    rem = tot - hyd
    return tot, hyd, rem

def set_queue_status(cur, url, status):
    # status values we use: 'hydrated', 'error', (optionally others later)
    cur.execute("UPDATE discovery_queue SET status=? WHERE url=?", (status, url))

def print_progress(cur, i_tag):
    tot, hyd, rem = db_counts(cur)
    print(f"{i_tag}  [progress] hydrated={hyd}  remaining={rem}  total={tot}")

# --- Main ---------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=800, help="rows to process")
    ap.add_argument("--pause", type=float, default=0.25, help="sleep between rows")
    ap.add_argument("--timeout", type=int, default=25, help="HTTP timeout (seconds)")
    ap.add_argument("--render-timeout", type=int, default=25, help="Render timeout (seconds)")
    ap.add_argument("--changed-only", action="store_true", help="skip update when hash unchanged")
    ap.add_argument("--min-html-len", type=int, default=1800, help="render if HTML length below this")
    ap.add_argument("--render-on", default="401,403,406,429", help="HTTP codes to force render on, comma-separated")
    ap.add_argument("--headed", action="store_true", help="launch browser window (debug)")
    ap.add_argument("--skip-assets", action="store_true", help="skip hydration for obvious assets/trackers")
    ap.add_argument("--no-pdf", action="store_true", help="also skip PDFs")
    args = ap.parse_args()

    render_on = {int(x.strip()) for x in (args.render_on or "").split(",") if x.strip().isdigit()}

    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Prefetch totals for nicer first-line progress
    total_before, hydrated_before, remaining_before = db_counts(cur)
    print(f"[init] discovery_queue: total={total_before} hydrated={hydrated_before} remaining={remaining_before}")

    # Oldest-first so we quickly fill any blank/new rows
    rows = cur.execute("""
        SELECT url,
               COALESCE(etag,'') AS et,
               COALESCE(last_modified,'') AS lm,
               COALESCE(content_hash,'') AS prev_hash,
               COALESCE(render_mode,'static') AS prev_render
          FROM documents
         WHERE (COALESCE(body,'')='' OR fetched_at IS NULL)
            OR (status_code NOT IN (200,304) OR last_error IS NOT NULL)
         ORDER BY datetime(COALESCE(fetched_at,'')) ASC
         LIMIT ?
    """, (args.limit,)).fetchall()

    processed = 0
    successes = 0
    rendered_ok = 0
    skipped_assets = 0
    errors = 0

    for i, (url, etag, last_mod, prev_hash, prev_render) in enumerate(rows, 1):
        processed += 1

        # Optional pre-filter
        if args.skip_assets and is_probably_asset(url, allow_pdf=(not args.no_pdf)):
            fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            cur.execute("""
                UPDATE documents SET
                    status_code=?,
                    render_mode=?,
                    fetched_at=?,
                    last_error=?,
                    retry_count=COALESCE(retry_count,0)+1
                WHERE url=?
            """, (0, "skipped-asset", fetched_at, "skipped-asset", url))
            # Consider assets "done" for queue purposes so they don't block progress
            set_queue_status(cur, url, "hydrated")
            con.commit()
            skipped_assets += 1
            print(f"[{i}] skip-asset {url}")
            print_progress(cur, f"[{i}]")
            time.sleep(args.pause)
            continue

        # --- Policy resolution for this URL (shadow-mode by default) -----------
        _policy = None
        if _POLICY_AVAILABLE:
            try:
                _policy = get_policy_for_url(url)
                _suggest = suggest_normalized_url(url, _policy)
                if _suggest != url:
                    print(f"[policy-shadow] {url}  ->  {_suggest}")
            except Exception:
                _policy = None

        # Derive effective knobs with CLI precedence; only override when enforcing
        _render_on_status = render_on
        if _POLICY_AVAILABLE and _policy and policy_enforce_enabled():
            try:
                if getattr(_policy.hydration, "render_on_status", None):
                    _render_on_status = set(_policy.hydration.render_on_status)
            except Exception:
                pass

        # Fetch static
        code, body, new_etag, new_lm, ctype = http_get(url, etag or None, last_mod or None, timeout=args.timeout)

        fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        used_render = False
        last_error = None

        # 304 short-circuit
        if code == 304:
            cur.execute("""
                UPDATE documents
                   SET status_code=?,
                       fetched_at=?,
                       etag=?,
                       last_modified=?,
                       last_error=NULL
                 WHERE url=?
            """, (304, fetched_at, new_etag or etag, new_lm or last_mod, url))
            set_queue_status(cur, url, "hydrated")
            con.commit()
            successes += 1
            print(f"[{i}] 304 {url}")
            print_progress(cur, f"[{i}]")
            time.sleep(args.pause); continue

        # Decide render: original heuristic OR (if enforcing) policy-based thin/script checks
        _min_len = args.min_html_len
        need_render = should_render(code, body, ctype, _render_on_status, _min_len)

        if not need_render and _POLICY_AVAILABLE and _policy and policy_enforce_enabled():
            try:
                _thin_min = _policy.hydration.render_if.thin_html_min_text_chars
                _script_over = _policy.hydration.render_if.script_ratio_over
                if _is_thin_html(body, _thin_min):
                    need_render = True
                elif _script_over is not None and _script_ratio(body) > float(_script_over):
                    need_render = True
            except Exception:
                pass

        if need_render:
            rendered, rstatus = try_render(
                url, timeout_ms=int(args.render_timeout * 1000), wait_after_ms=1200, headed=args.headed
            )
            if rstatus == "render-ok" and len(rendered) > len(body or ""):
                body = rendered
                used_render = True
                rendered_ok += 1

        effective_code = code
        if used_render and (code in _render_on_status or code in (0, 401, 403, 406, 429) or len(body) > 0):
            effective_code = 200

        content_hash = hashlib.md5((body or "").encode("utf-8","ignore")).hexdigest() if effective_code == 200 else None

        # If unchanged-only and we have the same content, still mark queue as hydrated
        if args.changed_only and prev_hash and content_hash == prev_hash:
            set_queue_status(cur, url, "hydrated")
            con.commit()
            successes += 1
            print(f"[{i}] unchanged {url}")
            print_progress(cur, f"[{i}]")
            time.sleep(args.pause); continue

        if code in (429, 503):
            # Back off a bit and record the issue; keep pending for retry
            time.sleep(min(5 + i*0.05, 15))
            last_error = f"http-{code}"

        if effective_code != 200 and code == 0:
            last_error = "network-error"

        cur.execute("""
            UPDATE documents SET
                body=?,
                clean_text=?,
                status_code=?,
                render_mode=?,
                fetched_at=?,
                content_hash=?,
                etag=?,
                last_modified=?,
                last_error=NULLIF(?, ''),
                retry_count=COALESCE(retry_count,0) + ?
            WHERE url=?
        """, (
            body if effective_code == 200 else "",
            "",  # downstream cleaner will fill
            effective_code,
            "render" if used_render else "static",
            fetched_at,
            content_hash,
            new_etag or etag,
            new_lm or last_mod,
            last_error or "",
            1 if effective_code not in (200, 304) else 0,
            url
        ))

        # Optional stamps (safe if columns missing)
        if used_render:
            try:
                cur.execute("UPDATE documents SET render_used=1 WHERE url=?", (url,))
            except Exception:
                pass
        if _POLICY_AVAILABLE and _policy:
            try:
                cur.execute("UPDATE documents SET policy_host=?, policy_version=? WHERE url=?",
                            (_policy.host, _policy.version, url))
            except Exception:
                pass

        # Queue status: mark hydrated only on success/304/unchanged; leave NULL on error/rate-limit
        if effective_code in (200, 304) and (body or "") != "" or (effective_code == 304):
            set_queue_status(cur, url, "hydrated")
            successes += 1
        elif last_error:
            # Keep pending for retry, but you could mark 'error' if you prefer:
            # set_queue_status(cur, url, "error")
            errors += 1

        con.commit()

        tag = "(render)" if used_render else ""
        print(f"[{i}] {effective_code:>3} {tag} {url}")
        print_progress(cur, f"[{i}]")
        time.sleep(args.pause)

    # Final summary
    tot_after, hyd_after, rem_after = db_counts(cur)
    con.close()

    print("\n[summary]")
    print(f" processed : {processed}")
    print(f" successes : {successes}")
    print(f" rendered  : {rendered_ok}")
    print(f" skipped   : {skipped_assets}")
    print(f" errors    : {errors}")
    print(f" queue     : hydrated={hyd_after}  remaining={rem_after}  total={tot_after}")
    print("[done] hydration pass complete.")
    
if __name__ == "__main__":
    main()
