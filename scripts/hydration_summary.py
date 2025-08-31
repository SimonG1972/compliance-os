#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Hydration summary:
- Groups by host (netloc with leading 'www.' stripped)
- Counts hydrated docs: status_code = 200 AND body != ''
- Sums content size (characters as stored; close enough for bytes ballpark)
- Estimates pages from text (words/500). Uses clean_text if present; otherwise
  strips tags from body on the fly.
- Prints per-host table and overall totals.

Usage:
    python scripts/hydration_summary.py
"""

import os
import re
import sqlite3
import argparse
import math
from urllib.parse import urlparse
from html import unescape

DB = os.path.join(os.getcwd(), "compliance.db")

TAG_RE = re.compile(r"<[^>]+>")
WS_RE  = re.compile(r"\s+")

def host_of(u: str) -> str:
    try:
        h = urlparse(u).netloc.lower()
    except Exception:
        return ""
    if h.startswith("www."):
        h = h[4:]
    return h

def strip_html_to_text(html: str) -> str:
    if not html:
        return ""
    # unescape entities first, then strip tags, normalize whitespace
    txt = unescape(html)
    txt = TAG_RE.sub(" ", txt)
    txt = WS_RE.sub(" ", txt).strip()
    return txt

def word_count(text: str) -> int:
    if not text:
        return 0
    # cheap but effective word split
    return len([w for w in text.split(" ") if w])

def human_size(nchars: int) -> str:
    # nchars is close to bytes for utf-8 legal pages; good enough for a rollup
    if nchars is None:
        nchars = 0
    mb = nchars / (1024.0 * 1024.0)
    if mb >= 1:
        return f"{mb:,.1f} MB"
    kb = nchars / 1024.0
    return f"{kb:,.0f} KB"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-status", type=int, default=200,
                    help="only count rows with status_code >= this (default 200)")
    ap.add_argument("--only-200", action="store_true",
                    help="restrict strictly to status_code == 200 (default behavior already)")
    ap.add_argument("--page-words", type=int, default=500,
                    help="words per page for estimates (default 500)")
    ap.add_argument("--limit-hosts", type=int, default=0,
                    help="show only top N hosts by doc count (0 = all)")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Pull hydrated rows (status 200 with body present)
    where = "status_code = 200 AND COALESCE(body,'') <> ''"
    params = []

    rows = cur.execute(f"""
        SELECT url,
               COALESCE(clean_text, '') AS clean_text,
               body
          FROM documents
         WHERE {where}
    """, params).fetchall()

    # Aggregate per host
    agg = {}  # host -> dict
    total_docs = 0
    total_chars = 0
    total_words = 0

    for url, clean_text, body in rows:
        h = host_of(url)
        if not h:
            continue

        entry = agg.setdefault(h, {
            "docs": 0,
            "chars": 0,
            "words": 0,
        })

        # prefer clean_text if present; else strip HTML
        if clean_text:
            text = clean_text
        else:
            # body is text (decoded) per hydrate_smart; strip tags
            text = strip_html_to_text(body or "")

        w = word_count(text)
        # chars: if clean_text present, use that length; else fallback to len(body)
        c = len(text) if clean_text else len(body or "")

        entry["docs"]  += 1
        entry["chars"] += c
        entry["words"] += w

        total_docs  += 1
        total_chars += c
        total_words += w

    # Sort hosts by doc count desc, then size desc
    items = sorted(agg.items(), key=lambda kv: (kv[1]["docs"], kv[1]["chars"]), reverse=True)
    if args.limit_hosts and args.limit_hosts > 0:
        items = items[:args.limit_hosts]

    # Print table
    if not items:
        print("No hydrated documents found (status_code=200 with non-empty body).")
        return

    # Header
    print(f"{'host':<32} {'docs':>6} {'size':>12} {'avg/doc':>10} {'est pages':>10}")
    print("-" * 74)

    for host, d in items:
        docs  = d["docs"]
        chars = d["chars"]
        words = d["words"]
        pages = math.ceil(words / float(args.page_words)) if words else 0
        avg   = (chars // docs) if docs else 0
        print(f"{host:<32} {docs:>6} {human_size(chars):>12} {human_size(avg):>10} {pages:>10,}")

    print("-" * 74)
    total_pages = math.ceil(total_words / float(args.page_words)) if total_words else 0
    avg_total   = (total_chars // total_docs) if total_docs else 0
    print(f"{'TOTAL':<32} {total_docs:>6} {human_size(total_chars):>12} {human_size(avg_total):>10} {total_pages:>10,}")

    con.close()

if __name__ == "__main__":
    main()
