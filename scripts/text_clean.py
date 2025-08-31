#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Text cleaner:
- Reads hydrated HTML from documents.body
- Produces documents.clean_text
- Two-pass strategy:
    1) aggressive: drop scripts/styles/noscript/template/svg/comments and structural chrome (header/nav/footer/aside)
    2) lenient: drop only scripts/styles/noscript/template/svg/comments, keep everything else
- If still too short but HTML is large, fall back to a minimal tag-strip path to avoid losing valuable content.
- Modes:
    --mode full     -> process all rows with non-null body
    --mode salvage  -> process only rows where clean_text is NULL or empty
"""

import re
import time
import html
import argparse
import sqlite3
from urllib.parse import urlparse

DEFAULT_DB = "compliance.db"

# ------------------------- HTML cleaners -------------------------

# Remove blocks that never contain user-visible text
RE_DROP_ALWAYS = re.compile(
    r"(?is)<(script|style|noscript|template|svg)[\s\S]*?</\1>"
)

# Remove HTML comments
RE_COMMENTS = re.compile(r"(?is)<!--[\s\S]*?-->")

# Aggressive structural chrome removal (safe tag names only)
RE_CHROME = re.compile(r"(?is)<(header|nav|footer|aside)[\s\S]*?</\1>")

# Newline for common block tags (to preserve structure)
RE_BLOCKNL = re.compile(
    r"(?is)</?(?:p|div|section|article|li|ul|ol|dl|dt|dd|table|thead|tbody|tfoot|tr|td|th|pre|blockquote|main|h[1-6])[^>]*>"
)

# <br> -> newline
RE_BRS = re.compile(r"(?i)<br\s*/?>")

# Any remaining tag
RE_TAGS = re.compile(r"(?s)<[^>]+>")

# Collapse whitespace
RE_WS = re.compile(r"[ \t\f\v]+")

# Collapse 3+ blank lines
RE_MLNL = re.compile(r"\n{3,}")

def _strip_common(html_text: str) -> str:
    """Remove always-hidden content and comments."""
    t = RE_DROP_ALWAYS.sub(" ", html_text)
    t = RE_COMMENTS.sub(" ", t)
    return t

def _normalize_text(txt: str) -> str:
    """Whitespace normalization and HTML unescape."""
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    txt = html.unescape(txt)
    txt = RE_WS.sub(" ", txt)
    txt = RE_BRS.sub("\n", txt)
    txt = RE_BLOCKNL.sub("\n", txt)
    txt = RE_TAGS.sub(" ", txt)
    # normalize whitespace lines
    txt = "\n".join(line.strip() for line in txt.split("\n"))
    txt = RE_MLNL.sub("\n\n", txt)
    return txt.strip()

def clean_aggressive(html_text: str) -> str:
    """Aggressive: drop scripts/styles/comments + structural chrome, then strip."""
    t = _strip_common(html_text)
    # structural chrome only by tag name (safe)
    t = RE_CHROME.sub(" ", t)
    return _normalize_text(t)

def clean_lenient(html_text: str) -> str:
    """Lenient: drop scripts/styles/comments only, keep all other DOM, then strip."""
    t = _strip_common(html_text)
    return _normalize_text(t)

def clean_fallback(html_text: str) -> str:
    """Last resort: minimal cleanup then strip tags; do not remove header/nav/etc."""
    t = _strip_common(html_text)
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = RE_BRS.sub("\n", t)
    t = RE_TAGS.sub(" ", t)  # brutally strip tags
    t = html.unescape(t)
    t = RE_WS.sub(" ", t)
    t = "\n".join(line.strip() for line in t.split("\n"))
    t = RE_MLNL.sub("\n\n", t)
    return t.strip()

def smart_clean(html_text: str, min_chars: int = 120, big_html_threshold: int = 5000):
    """
    Try aggressive -> lenient -> fallback. Return (cleaned_text, strategy_used).
    We only accept a result if it meets min_chars, else we try the next strategy.
    If the HTML is large but the cleaned text is short, we force fallback to avoid losing content.
    """
    # Pass 1: aggressive
    a = clean_aggressive(html_text)
    if len(a) >= min_chars:
        return a, "aggressive"

    # Pass 2: lenient
    l = clean_lenient(html_text)
    if len(l) >= min_chars or len(l) > len(a):
        if len(l) >= min_chars:
            return l, "lenient"

    # Pass 3: fallback for large HTML or as last resort
    if len(html_text) >= big_html_threshold:
        f = clean_fallback(html_text)
        return f, "fallback"

    # If still small HTML, keep the best of a/lenient
    best = l if len(l) > len(a) else a
    return best, "best-effort"

# ------------------------- DB runner -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB (default: compliance.db)")
    ap.add_argument("--mode", choices=["full", "salvage"], default="full",
                    help="full = clean all hydrated docs; salvage = only rows with NULL/empty clean_text")
    ap.add_argument("--min-chars", type=int, default=120,
                    help="Minimum characters to consider a cleaned page valid (default: 120)")
    ap.add_argument("--limit", type=int, default=0, help="Optional limit of rows to process")
    ap.add_argument("--commit-every", type=int, default=500, help="Commit interval (default: 500)")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.execute("PRAGMA journal_mode=WAL")
    cur = con.cursor()

    if args.mode == "full":
        sql = "SELECT id, url, body FROM documents WHERE body IS NOT NULL"
        params = []
    else:
        sql = "SELECT id, url, body FROM documents WHERE body IS NOT NULL AND (clean_text IS NULL OR LENGTH(clean_text)=0)"
        params = []

    if args.limit and args.limit > 0:
        sql += " LIMIT ?"
        params.append(args.limit)

    rows = cur.execute(sql, params).fetchall()

    t0 = time.time()
    processed = 0
    updated = 0
    empty_after = 0

    host_empty = {}  # host -> count
    strategy_counts = {"aggressive": 0, "lenient": 0, "fallback": 0, "best-effort": 0}

    for i, (doc_id, url, body) in enumerate(rows, 1):
        processed += 1
        host = urlparse(url).netloc.lower() if url else ""
        html_text = body or ""
        cleaned, strat = smart_clean(html_text, min_chars=args.min_chars)
        strategy_counts[strat] = strategy_counts.get(strat, 0) + 1

        if cleaned:
            cur.execute("UPDATE documents SET clean_text=? WHERE id=?", (cleaned, doc_id))
            updated += 1
        else:
            empty_after += 1
            host_empty[host] = host_empty.get(host, 0) + 1

        if (i % args.commit_every) == 0:
            con.commit()

    con.commit()

    # Summary
    t1 = time.time()
    print("\n[text_clean] summary")
    print("==============================================================")
    print(f"processed:       {processed}")
    print(f"updated clean:   {updated}")
    print(f"left NULL/empty: {empty_after}")
    print(f"time:            {t1 - t0:.1f}s")
    print("--------------------------------------------------------------")

    # Global counts
    total_docs = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    with_clean = cur.execute("SELECT COUNT(*) FROM documents WHERE clean_text IS NOT NULL AND LENGTH(clean_text)>0").fetchone()[0]
    ge_min = cur.execute("SELECT COUNT(*) FROM documents WHERE clean_text IS NOT NULL AND LENGTH(clean_text) >= ?", (args.min_chars,)).fetchone()[0]
    print(f"docs total:      {total_docs}")
    print(f"with clean_text: {with_clean}")
    print(f"clean >= {args.min_chars} chars: {ge_min}")

    # Strategy breakdown
    print("\n[strategy usage]")
    for k in ("aggressive", "lenient", "fallback", "best-effort"):
        print(f"{k:12s} {strategy_counts.get(k,0)}")

    # Top hosts with null
    if host_empty:
        print("\n[top hosts with NULL/empty clean_text]")
        for h, c in sorted(host_empty.items(), key=lambda kv: kv[1], reverse=True)[:20]:
            print(f"{h:30s} {c}")

    con.close()

if __name__ == "__main__":
    main()
