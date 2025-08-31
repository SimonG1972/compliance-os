#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Chunk a specific document (or a tiny filtered set) for quick inspection.

- Select by --id, or --url LIKE, or a custom --where filter
- Prints a small preview of the first N chunks (--show)
- Shares the same splitting logic as the batch script
- Supports --rechunk, --dry-run, and the same sizing flags

Examples:
  python scripts/chunk_docs.py --id 123 --show 3 --rechunk
  python scripts/chunk_docs.py --url-like '%instagram.com/legal/privacy%' --show 2
  python scripts/chunk_docs.py --where "host LIKE '%giphy.com%'" --limit 2 --dry-run
"""

import os, re, sys, sqlite3, argparse
from urllib.parse import urlparse

DB_DEFAULT = os.path.join(os.getcwd(), "compliance.db")

HEADING_LINE = re.compile(
    r"^\s*(?:#{1,6}\s+|[0-9]{1,2}\.|[IVX]{1,5}\.|Appendix\b|Section\b|Article\b|Privacy Policy\b|Terms(?: of Service)?\b)",
    re.I
)
PARA_SPLIT = re.compile(r"\n\s*\n+")
SENT_SPLIT = re.compile(r"(?<=[\.\?\!])[\t ]+(?=[A-Z(])|(?<=[\.\?\!])\n+(?=\S)")
WS_COMPACT = re.compile(r"[ \t]+")

def normalize_text(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def split_headings(text: str):
    lines = text.split("\n")
    blocks, cur = [], []
    for ln in lines:
        if HEADING_LINE.search(ln) and cur:
            blocks.append("\n".join(cur).strip())
            cur = [ln]
        else:
            cur.append(ln)
    if cur:
        blocks.append("\n".join(cur).strip())
    return [b for b in blocks if b]

def split_paragraphs(block: str):
    return [p.strip() for p in PARA_SPLIT.split(block) if p.strip()]

def split_sentences(paragraph: str):
    parts = [p.strip() for p in SENT_SPLIT.split(paragraph) if p.strip()]
    return parts if parts else [paragraph.strip()]

def make_chunks_from_sentences(sents, max_chars: int, overlap: int):
    chunks = []
    buf, buf_len = [], 0

    def flush():
        nonlocal buf, buf_len
        if not buf:
            return
        text = " ".join(buf)
        text = WS_COMPACT.sub(" ", text).strip()
        if text:
            chunks.append(text)
        buf, buf_len = [], 0

    for s in sents:
        s = s.strip()
        if not s: 
            continue

        if len(s) > max_chars:
            flush()
            start = 0
            while start < len(s):
                end = min(start + max_chars, len(s))
                chunk = s[start:end].strip()
                if chunk:
                    chunks.append(chunk)
                start = end - overlap if overlap > 0 else end
                if start < 0:
                    start = 0
        else:
            add_len = (1 if buf else 0) + len(s)
            if buf_len + add_len <= max_chars:
                if buf:
                    buf.append(s); buf_len += add_len
                else:
                    buf = [s]; buf_len = len(s)
            else:
                flush()
                buf, buf_len = [s], len(s)

    flush()

    if overlap <= 0 or len(chunks) <= 1:
        return chunks

    overlapped = []
    for i, ch in enumerate(chunks):
        if i == 0:
            overlapped.append(ch); continue
        prev = overlapped[-1]
        tail = prev[-overlap:] if overlap < len(prev) else prev
        merged = (tail + " " + ch).strip()
        overlapped.append(merged if len(merged) <= int(max_chars * 1.15) else ch)
    return overlapped

def ensure_chunks_table(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            text TEXT NOT NULL,
            char_len INTEGER NOT NULL,
            token_estimate INTEGER NOT NULL,
            url TEXT,
            host TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_doc ON chunks(doc_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_host ON chunks(host)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_docidx ON chunks(doc_id, chunk_index)")
    con.commit()

def delete_chunks_for_doc(cur: sqlite3.Cursor, doc_id: int):
    cur.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))

def estimate_tokens_from_chars(char_len: int) -> int:
    return max(1, int(round(char_len / 4.0)))

def chunk_doc_row(cur, doc_row, max_chars, overlap, rechunk=False, dry_run=False, show=0):
    doc_id, url, clean_text = doc_row
    host = (urlparse(url).netloc or "").lower()

    if not clean_text or len(clean_text) == 0:
        print(f"[skip] doc_id={doc_id} empty clean_text")
        return 0

    if rechunk and not dry_run:
        delete_chunks_for_doc(cur, doc_id)

    text = normalize_text(clean_text)
    sentences = []
    for b in split_headings(text):
        for p in split_paragraphs(b):
            sentences.extend(split_sentences(p))
    if not sentences:
        sentences = [text]

    chunks = make_chunks_from_sentences(sentences, max_chars, overlap)
    rows = []
    for i, ch in enumerate(chunks):
        c_len = len(ch)
        t_est = estimate_tokens_from_chars(c_len)
        rows.append((doc_id, i, ch, c_len, t_est, url, host))

    if not dry_run:
        cur.executemany(
            "INSERT INTO chunks (doc_id, chunk_index, text, char_len, token_estimate, url, host) "
            "VALUES (?,?,?,?,?,?,?)",
            rows,
        )

    print(f"[ok] doc_id={doc_id} host={host} url={url}\n     chunks={len(rows)} avg_len={sum(r[3] for r in rows)//max(1,len(rows))} max_len={max(r[3] for r in rows)}")
    if show > 0:
        for j, r in enumerate(rows[:show]):
            print(f"  --- chunk {j} ({r[3]} chars) ---")
            snippet = r[2][:300].replace("\n", " ")
            print(f"  {snippet} {'...' if len(r[2])>300 else ''}")
    return len(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--id", type=int, help="documents.id to chunk")
    ap.add_argument("--url-like", default="", help="URL LIKE pattern (e.g., '%%privacy%%')")
    ap.add_argument("--where", default="", help="Custom SQL AND filter over documents")
    ap.add_argument("--limit", type=int, default=1, help="Limit for multi-select")
    ap.add_argument("--max-chars", type=int, default=2200)
    ap.add_argument("--overlap", type=int, default=250)
    ap.add_argument("--min-chars", type=int, default=100)
    ap.add_argument("--rechunk", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show", type=int, default=2, help="Preview first N chunks")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    ensure_chunks_table(con)
    cur = con.cursor()

    if args.id:
        rows = cur.execute("SELECT id, url, clean_text FROM documents WHERE id=? AND clean_text IS NOT NULL AND length(clean_text)>=?", (args.id, args.min_chars)).fetchall()
    else:
        filters = ["clean_text IS NOT NULL", "length(clean_text)>=?"]
        params = [args.min_chars]
        if args.url_like:
            filters.append("url LIKE ?")
            params.append(args.url_like)
        if args.where.strip():
            filters.append("(" + args.where.strip() + ")")
        sql = f"SELECT id, url, clean_text FROM documents WHERE {' AND '.join(filters)} ORDER BY id LIMIT ?"
        params.append(args.limit)
        rows = cur.execute(sql, params).fetchall()

    if not rows:
        print("No matching documents found.")
        return

    total = 0
    for row in rows:
        total += chunk_doc_row(cur, row, args.max_chars, args.overlap, rechunk=args.rechunk, dry_run=args.dry_run, show=args.show)
    if not args.dry_run:
        con.commit()
    print(f"\nDONE. docs={len(rows)} total_chunks={total} mode={'DRY-RUN' if args.dry_run else 'WRITE'}")

if __name__ == "__main__":
    main()
