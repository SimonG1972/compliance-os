#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Streaming, low-RAM chunker.
- Inserts chunks in small batches; never builds huge lists in RAM.
- Respects "resume" by default (skips docs already chunked).
- Lets you exclude hosts/ids, set a max chunk cap per doc, and throttle.

Examples:
  # Preview 10 docs that would be processed
  python scripts\\chunk_docs_safe.py --limit 10 --preview 1

  # Chunk everything ready, skipping TikTok
  python scripts\\chunk_docs_safe.py --max-chars 800 --overlap 120 --min-chars 120 ^
    --where "clean_text IS NOT NULL AND length(clean_text) >= 120" ^
    --exclude-host-like tiktok.com --resume 1 --show 0

  # Stream-chunk just a couple of monster ids with bigger windows, cap chunks
  python scripts\\chunk_docs_safe.py --where "id IN (60,162)" --max-chars 2000 --overlap 200 ^
    --max-chunks-per-doc 600 --show 1
"""
import os, sqlite3, argparse, time

DB = os.path.join(os.getcwd(), "compliance.db")

def ensure_chunk_table(cur):
    # create if needed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY,
            doc_id INTEGER,
            chunk_index INTEGER,
            text TEXT,
            char_len INTEGER,
            token_estimate INTEGER,
            url TEXT,
            host TEXT,
            created_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON chunks(doc_id)")
    # add columns if older schema
    cols = {r[1] for r in cur.execute("PRAGMA table_info(chunks)")}
    if "char_len" not in cols:
        cur.execute("ALTER TABLE chunks ADD COLUMN char_len INTEGER")
    if "token_estimate" not in cols:
        cur.execute("ALTER TABLE chunks ADD COLUMN token_estimate INTEGER")

def host_of(url: str) -> str:
    try:
        return url.split("://",1)[1].split("/",1)[0].lower()
    except Exception:
        return ""

def token_estimate(chars: int) -> int:
    # fast heuristic
    return max(1, chars // 4)

def chunk_stream(text: str, max_chars: int, overlap: int):
    """
    Simple sliding window chunker (char-based) with overlap.
    Yields chunk strings; doesn't keep them all in memory.
    """
    if not text:
        return
    n = len(text)
    start = 0
    while start < n:
        end = min(start + max_chars, n)
        yield text[start:end]
        if end >= n:
            break
        start = max(end - overlap, start + 1)

def should_skip_host(url: str, excludes) -> bool:
    if not excludes: return False
    h = host_of(url)
    for pat in excludes:
        if pat.lower() in h:
            return True
    return False

def build_where(base_where: str, resume: bool, exclude_ids, exclude_host_like):
    where = base_where.strip() if base_where.strip() else "1=1"
    if resume:
        where += " AND id NOT IN (SELECT DISTINCT doc_id FROM chunks)"
    if exclude_ids:
        where += f" AND id NOT IN ({','.join(str(i) for i in exclude_ids)})"
    # note: we also filter hosts in Python to avoid LIKE wildcards pitfalls
    return where

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100000)
    ap.add_argument("--where", default="clean_text IS NOT NULL AND length(clean_text) >= 120")
    ap.add_argument("--max-chars", type=int, default=800)
    ap.add_argument("--overlap", type=int, default=120)
    ap.add_argument("--min-chars", type=int, default=120)
    ap.add_argument("--batch", type=int, default=200, help="rows per executemany insert")
    ap.add_argument("--sleep-ms", type=int, default=20, help="pause between docs (ms)")
    ap.add_argument("--max-chunks-per-doc", type=int, default=0, help="0 = no cap")
    ap.add_argument("--resume", type=int, default=1, help="1=skip docs already chunked")
    ap.add_argument("--exclude-host-like", nargs="*", default=[], help="e.g. tiktok.com policies.google.com")
    ap.add_argument("--exclude-ids", nargs="*", type=int, default=[])
    ap.add_argument("--preview", type=int, default=0, help="1=print selected docs and exit")
    ap.add_argument("--show", type=int, default=0, help="1=per-doc print")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()
    ensure_chunk_table(cur)

    where_sql = build_where(args.where, bool(args.resume), args.exclude_ids, args.exclude_host_like)

    rows = cur.execute(f"""
        SELECT id, url, clean_text
        FROM documents
        WHERE {where_sql}
        ORDER BY id
        LIMIT ?
    """, (args.limit,)).fetchall()

    # Python-level host filter to avoid SQL LIKE quoting hassles
    if args.exclude_host_like:
        filt = []
        for (doc_id, url, clean_text) in rows:
            if should_skip_host(url, args.exclude_host_like):
                continue
            filt.append((doc_id, url, clean_text))
        rows = filt

    if args.preview:
        print(f"[preview] {len(rows)} docs would be processed. First 10:")
        for r in rows[:10]:
            print("  ", r[0], host_of(r[1]), r[1])
        con.close()
        return

    processed = 0
    total_chunks = 0

    for (doc_id, url, clean_text) in rows:
        if not clean_text or len(clean_text) < args.min_chars:
            continue

        host = host_of(url)
        idx = 0
        batch = []
        per_doc_chunks = 0

        for ck in chunk_stream(clean_text, args.max_chars, args.overlap):
            if len(ck) < args.min_chars:
                continue
            if args.max_chunks_per_doc and idx >= args.max_chunks_per_doc:
                break

            batch.append((doc_id, idx, ck, len(ck), token_estimate(len(ck)), url, host, time.strftime("%Y-%m-%dT%H:%M:%S")))
            idx += 1
            per_doc_chunks += 1
            total_chunks += 1

            if len(batch) >= args.batch:
                cur.executemany(
                    "INSERT INTO chunks (doc_id, chunk_index, text, char_len, token_estimate, url, host, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    batch
                )
                batch.clear()

        if batch:
            cur.executemany(
                "INSERT INTO chunks (doc_id, chunk_index, text, char_len, token_estimate, url, host, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                batch
            )

        con.commit()
        processed += 1
        if args.show:
            print(f"[ok] doc_id={doc_id} host={host} chunks={per_doc_chunks} url={url}")

        # tiny pause to keep things gentle on the system
        if args.sleep_ms:
            time.sleep(args.sleep_ms / 1000.0)

    con.close()
    print(f"\nDONE. docs={processed} total_chunks={total_chunks} mode=SAFE")

if __name__ == "__main__":
    main()
