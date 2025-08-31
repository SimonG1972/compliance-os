#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Chunk all hydrated documents into overlapping text chunks.
- Prefers documents.clean_text; if empty/NULL, falls back to cleaned(body).
- Skips docs that already have chunks unless --rechunk is set.
- Inserts only columns that exist in the `chunks` table (auto-detect).
- Prints a run summary and per-host breakdown.

Usage examples:
  python scripts/chunk_documents.py
  python scripts/chunk_documents.py --max-chars 2200 --overlap 250 --min-chars 300
  python scripts/chunk_documents.py --rechunk
  python scripts/chunk_documents.py --where "host LIKE '%instagram.com%'"
"""

import os
import re
import time
import math
import html
import argparse
import sqlite3
import urllib.parse
from typing import List, Tuple, Dict

DB_PATH = os.path.join(os.getcwd(), "compliance.db")

# ----------------------------- helpers -----------------------------

def now_ms() -> int:
    return int(time.time() * 1000)

def host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""

# very lightweight HTML -> text (used only when clean_text is missing)
BLOCK_TAGS = ("p","div","section","article","li","ul","ol","h1","h2","h3","h4","h5","h6","header","footer","nav","tr","table")
BLOCK_RE = re.compile(r"</?(%s)\b[^>]*>" % "|".join(BLOCK_TAGS), re.I)
BR_RE = re.compile(r"<br\s*/?>", re.I)
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style|noscript|template|svg|canvas|iframe)[^>]*>.*?</\1>")

ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\u2060\uFEFF]")
MULTI_NL_RE = re.compile(r"\n{3,}")
MULTI_WS_RE = re.compile(r"[ \t]{2,}")

def html_to_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    s = SCRIPT_STYLE_RE.sub("", raw_html)
    s = BR_RE.sub("\n", s)
    s = BLOCK_RE.sub("\n", s)
    s = TAG_RE.sub("", s)
    s = html.unescape(s)
    s = ZERO_WIDTH_RE.sub("", s)

    # normalize lines
    lines = []
    for line in s.splitlines():
        line = line.strip()
        if not line:
            lines.append("")
            continue
        # trim obvious boilerplate tails we often see
        if len(line) > 20000:
            line = line[:20000]
        lines.append(line)

    s = "\n".join(lines)
    s = MULTI_WS_RE.sub(" ", s)
    s = MULTI_NL_RE.sub("\n\n", s)
    return s.strip()

def pick_text(clean_text: str, body: str) -> Tuple[str, str]:
    """
    Return (text, source) where source is 'clean' or 'body-fallback'
    """
    if clean_text and clean_text.strip():
        return clean_text.strip(), "clean"
    if body and body.strip():
        return html_to_text(body), "body-fallback"
    return "", "none"

# splitting logic: greedy, try to break on paragraph/period boundaries
PREF_BREAKS = ["\n\n", "\n", ". ", "。", "! ", "? ", "; "]

def split_chunks(text: str, max_chars: int, min_chars: int, overlap: int) -> List[str]:
    chunks = []
    n = len(text)
    i = 0
    if n == 0:
        return chunks

    while i < n:
        end = min(i + max_chars, n)
        j = end

        if end - i >= min_chars:
            # try to find a nice boundary between [i+min_chars, end]
            window_start = i + min_chars
            window = text[window_start:end]

            # search from the end of window backwards for preferred breaks
            chosen = None
            for brk in PREF_BREAKS:
                pos = window.rfind(brk)
                if pos != -1:
                    candidate = window_start + pos + (0 if brk.strip() == "" else len(brk))
                    if candidate > i:
                        chosen = candidate
                        break
            if chosen:
                j = chosen

        # ensure progress
        if j <= i:
            j = min(i + max_chars, n)

        chunk = text[i:j].strip()
        if chunk:
            chunks.append(chunk)

        if j >= n:
            break

        # advance with overlap
        if overlap > 0:
            i = max(0, j - overlap)
            if i <= len(text) and i <= j:
                # avoid infinite loop if overlap is too big & chunk was tiny
                if j - i < 1:
                    i = j
        else:
            i = j

    # If last chunk is too small and can be merged with the previous, do it.
    if len(chunks) >= 2 and len(chunks[-1]) < max(1, min_chars // 2):
        chunks[-2] = (chunks[-2] + ("\n\n" if not chunks[-2].endswith("\n") else "") + chunks[-1]).strip()
        chunks.pop()

    return chunks

def token_estimate(chars: int) -> int:
    # very rough 4 chars/token heuristic
    return max(1, int(math.ceil(chars / 4.0)))

def table_columns(con: sqlite3.Connection, table: str) -> Dict[str, str]:
    cols = {}
    cur = con.cursor()
    for cid, name, coltype, notnull, dflt, pk in cur.execute(f"PRAGMA table_info({table})"):
        cols[name] = (coltype or "").upper()
    return cols

def has_existing_chunks(cur: sqlite3.Cursor, doc_id: int) -> bool:
    cur.execute("SELECT 1 FROM chunks WHERE doc_id = ? LIMIT 1", (doc_id,))
    return cur.fetchone() is not None

def delete_chunks_for(cur: sqlite3.Cursor, doc_id: int) -> None:
    cur.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))

# ----------------------------- main -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH, help="Path to compliance.db")
    ap.add_argument("--max-chars", type=int, default=2200, help="Max characters per chunk (default: 2200)")
    ap.add_argument("--min-chars", type=int, default=300, help="Minimum characters before trying a soft break (default: 300)")
    ap.add_argument("--overlap", type=int, default=250, help="Overlap characters between consecutive chunks (default: 250)")
    ap.add_argument("--where", help='Extra WHERE clause to limit documents, e.g. "url LIKE \'%instagram.com%\'"')
    ap.add_argument("--limit", type=int, default=0, help="Limit number of documents processed")
    ap.add_argument("--rechunk", action="store_true", help="Delete existing chunks for a doc and re-insert")
    args = ap.parse_args()

    start = now_ms()
    con = sqlite3.connect(args.db)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")

    # discover columns available in chunks table
    chunk_cols = table_columns(con, "chunks")
    must_have = {"doc_id", "chunk_index", "text"}
    if not must_have.issubset(set(chunk_cols.keys())):
        raise SystemExit("chunks table must have at least columns: doc_id, chunk_index, text")

    # build SELECT for documents
    base_where = "( (clean_text IS NOT NULL AND LENGTH(TRIM(clean_text)) > 0) OR (body IS NOT NULL AND LENGTH(TRIM(body)) > 0) )"
    if args.where:
        where_clause = f"{base_where} AND ({args.where})"
    else:
        where_clause = base_where

    limit_clause = f" LIMIT {int(args.limit)}" if args.limit and args.limit > 0 else ""
    sql = f"""
        SELECT id, url, title, clean_text, body
        FROM documents
        WHERE {where_clause}
        ORDER BY id ASC
        {limit_clause}
    """

    cur = con.cursor()
    rows = list(cur.execute(sql))

    processed = 0
    inserted_total = 0
    skipped_short = 0
    skipped_existing = 0

    per_host_docs = {}
    per_host_chunks = {}

    # fast existence check prepared statement
    chk_cur = con.cursor()
    del_cur = con.cursor()
    ins_cur = con.cursor()

    def insert_for_doc(doc_id: int, url: str, chunks: List[str]):
        nonlocal inserted_total
        # prepare dynamic INSERT based on available columns
        # always include: doc_id, chunk_index, text
        extra_cols = []
        if "token_estimate" in chunk_cols:
            extra_cols.append("token_estimate")
        if "url" in chunk_cols:
            extra_cols.append("url")
        if "title" in chunk_cols:
            extra_cols.append("title")

        columns = ["doc_id", "chunk_index", "text"] + extra_cols
        placeholders = ",".join(["?"] * len(columns))
        stmt = f"INSERT INTO chunks ({','.join(columns)}) VALUES ({placeholders})"

        # We might commit in batches for performance.
        batch = []
        for idx, ch in enumerate(chunks):
            vals = [doc_id, idx, ch]
            if "token_estimate" in extra_cols:
                vals.append(token_estimate(len(ch)))
            if "url" in extra_cols:
                vals.append(url)
            if "title" in extra_cols:
                # title is not selected for fallback if empty, but we include whatever we loaded
                # We will have it available via outer scope if we keep it; just leave blank here since not in closure.
                pass  # we'll handle below by re-building vals

            # rebuild vals correctly with title if needed
            if "title" in extra_cols:
                # title is the last extra col; append empty if we don't have it captured
                if len(vals) == 4 and "token_estimate" in extra_cols and "url" not in extra_cols:
                    # token_estimate appended; next is title
                    vals.append("")  # title
                elif len(vals) == 4 and "url" in extra_cols and "token_estimate" not in extra_cols:
                    # url appended; next is title
                    vals.append("")  # title
                elif len(vals) == 5 and "token_estimate" in extra_cols and "url" in extra_cols:
                    vals.append("")  # title after token_estimate + url
                elif "token_estimate" not in extra_cols and "url" not in extra_cols:
                    # no extras before title => title should be 4th item
                    vals.append("")  # title

            batch.append(vals)

            if len(batch) >= 500:
                ins_cur.executemany(stmt, batch)
                batch.clear()
        if batch:
            ins_cur.executemany(stmt, batch)
        con.commit()
        inserted_total += len(chunks)

    for (doc_id, url, title, clean_text, body) in rows:
        processed += 1
        h = host_of(url)

        # skip if already chunked (unless --rechunk)
        if not args.rechunk and has_existing_chunks(chk_cur, doc_id):
            skipped_existing += 1
            per_host_docs[h] = per_host_docs.get(h, 0) + 1
            continue

        # delete old chunks if rechunk
        if args.rechunk:
            delete_chunks_for(del_cur, doc_id)

        txt, source = pick_text(clean_text, body)
        if not txt or len(txt) < max(1, args.min_chars // 2):
            skipped_short += 1
            per_host_docs[h] = per_host_docs.get(h, 0) + 1
            continue

        chunks = split_chunks(txt, args.max_chars, args.min_chars, args.overlap)
        if not chunks:
            skipped_short += 1
            per_host_docs[h] = per_host_docs.get(h, 0) + 1
            continue

        # Insert
        insert_for_doc(doc_id, url, chunks)

        # per-host tallies
        per_host_docs[h] = per_host_docs.get(h, 0) + 1
        per_host_chunks[h] = per_host_chunks.get(h, 0) + len(chunks)

    dur = (now_ms() - start) / 1000.0

    # summary
    print("\nchunking summary")
    print("=" * 62)
    print(f"processed docs:   {processed}")
    print(f"inserted chunks:  {inserted_total}")
    print(f"skipped (short):  {skipped_short}")
    print(f"skipped (had chunks and no --rechunk): {skipped_existing}")
    print(f"time:             {dur:.1f}s\n")

    # per-host table (top 20 by chunks)
    rows = [(h or "(no-host)", per_host_docs.get(h, 0), per_host_chunks.get(h, 0)) for h in set(per_host_docs) | set(per_host_chunks)]
    rows.sort(key=lambda x: x[2], reverse=True)

    if rows:
        print("per-host (top 20 by chunks)")
        print(f"{'host':32}  {'docs':>5}  {'chunks':>6}")
        print(f"{'-'*32}  {'-'*5}  {'-'*6}")
        for h, dcnt, ccnt in rows[:20]:
            print(f"{h:32}  {dcnt:5d}  {ccnt:6d}")
        print("=" * 62)
    else:
        print("per-host (top 20 by chunks)")
        print(f"{'host':32}  {'docs':>5}  {'chunks':>6}")
        print(f"{'-'*32}  {'-'*5}  {'-'*6}")
        print("=" * 62)

if __name__ == "__main__":
    main()
