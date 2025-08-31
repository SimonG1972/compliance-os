# scripts/hybrid_export.py
import argparse
import csv
import os
import re
import sqlite3
from math import isfinite
from typing import List, Tuple

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    _HAS_ST = True
except Exception:
    _HAS_ST = False

def parse_near(near: str | None):
    if not near:
        return None
    a, b, w = near.split(":")
    return a.strip().lower(), b.strip().lower(), int(w)

_word = re.compile(r"\w+", re.UNICODE)
def within_window(text: str, a: str, b: str, win: int) -> bool:
    toks = [t.lower() for t in _word.findall(text)]
    pos_a = [i for i, t in enumerate(toks) if t == a]
    pos_b = [i for i, t in enumerate(toks) if t == b]
    if not pos_a or not pos_b:
        return False
    for i in pos_a:
        for j in pos_b:
            if abs(i - j) <= win:
                return True
    return False

def l2_normalize(v):
    n = (v**2).sum() ** 0.5
    return v if n == 0 else v / n

def score_and_rank(rows: List[Tuple[str,int,str,float,str]], query: str, alpha: float, near):
    # rows: (url, chunk_index, snippet, fts_rank, chunk_text)
    if near:
        a, b, w = near
        rows = [r for r in rows if within_window(r[4], a, b, w)]
    if not rows:
        return []

    import numpy as np
    ranks = np.array([r[3] for r in rows], dtype=float)
    ranks = np.where(np.isnan(ranks), np.nanmedian(ranks), ranks)
    inv = -ranks
    inv -= np.min(inv)
    if np.max(inv) > 0:
        inv /= np.max(inv)

    if _HAS_ST:
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        qv = l2_normalize(model.encode([query], convert_to_numpy=True)[0])
        cvecs = model.encode([r[4] for r in rows], convert_to_numpy=True, batch_size=64, show_progress_bar=False)
        norms = np.linalg.norm(cvecs, axis=1, keepdims=True) + 1e-12
        cvecs = cvecs / norms
        sims = cvecs @ qv
        sims -= np.min(sims)
        if np.max(sims) > 0:
            sims /= np.max(sims)
    else:
        sims = 0.0 * inv

    final = alpha * inv + (1 - alpha) * sims
    order = np.argsort(-final)
    ranked = [(float(final[i]), *rows[i][:4]) for i in order]  # (score, url, idx, snippet, fts_rank)
    return ranked

def main():
    ap = argparse.ArgumentParser(description="Hybrid search export to CSV.")
    ap.add_argument("query", help="FTS5 query")
    ap.add_argument("out_csv", help="Output CSV path")
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    ap.add_argument("--k", type=int, default=200)
    ap.add_argument("--alpha", type=float, default=0.55)
    ap.add_argument("--near", help="termA:termB:window (e.g., children:data:25)")
    ap.add_argument("--fetch-mult", type=int, default=5)
    args = ap.parse_args()

    near = parse_near(args.near)
    fetch_n = max(args.k * max(1, args.fetch_mult), args.k)

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = cur.execute(
        """
        WITH fts_hits AS (
          SELECT
            document_chunks_fts.rowid AS rid,
            rank                       AS fts_rank,
            snippet(document_chunks_fts, 1, '[', ']', ' … ', 10) AS snip
          FROM document_chunks_fts
          WHERE document_chunks_fts MATCH ?
          ORDER BY rank
          LIMIT ?
        )
        SELECT
          c.url,
          c.chunk_index,
          fh.snip,
          fh.fts_rank,
          c.chunk_text
        FROM fts_hits fh
        JOIN document_chunks c
          ON c.id = fh.rid
        """,
        (args.query, fetch_n),
    ).fetchall()
    con.close()

    tup = [(r["url"], r["chunk_index"], r["snip"], r["fts_rank"], r["chunk_text"]) for r in rows]
    ranked = score_and_rank(tup, args.query, alpha=args.alpha, near=near)[: args.k]

    if not ranked:
        print("No matches.")
        return

    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank", "score", "url", "chunk_index", "snippet", "fts_rank"])
        for i, (score, url, idx, snip, fts_rank) in enumerate(ranked, start=1):
            w.writerow([i, f"{score:.6f}", url, idx, snip.strip(), f"{fts_rank:.6f}"])
    print(f"Wrote {args.out_csv} ({len(ranked)} rows)")

if __name__ == "__main__":
    main()
