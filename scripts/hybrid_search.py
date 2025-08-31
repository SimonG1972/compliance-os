# scripts/hybrid_search.py
import argparse
import os
import re
import sqlite3
from math import isfinite
from typing import List, Tuple

# Optional hybrid re-rank
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
    if n == 0 or not isfinite(n):
        return v
    return v / n


def score_and_rank(
    rows: List[Tuple[str, int, str, float, str]],
    query: str,
    alpha: float = 0.55,
    near=None,
):
    # rows: (url, chunk_index, snippet, fts_rank, chunk_text)
    # Proximity guardrail
    if near:
        term_a, term_b, win = near
        rows = [r for r in rows if within_window(r[4], term_a, term_b, win)]
    if not rows:
        return []

    # Convert FTS rank (lower is better) into a normalized "higher is better" 0..1
    import numpy as np
    ranks = np.array([r[3] for r in rows], dtype=float)
    # Handle None/NaN gracefully
    if np.any(np.isnan(ranks)):
        med = np.nanmedian(ranks)
        ranks = np.where(np.isnan(ranks), med, ranks)
    inv = -ranks  # lower rank => higher score
    inv = inv - np.min(inv)
    if np.max(inv) > 0:
        inv = inv / np.max(inv)

    # Semantic similarity (optional)
    if _HAS_ST:
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        q_vec = l2_normalize(model.encode([query], convert_to_numpy=True)[0])
        c_texts = [r[4] for r in rows]
        c_vecs = model.encode(c_texts, convert_to_numpy=True, batch_size=64, show_progress_bar=False)
        c_vecs = (c_vecs.T / (np.linalg.norm(c_vecs, axis=1) + 1e-12)).T
        sims = c_vecs @ q_vec  # cosine similarity
        sims = sims - np.min(sims)
        if np.max(sims) > 0:
            sims = sims / np.max(sims)
    else:
        sims = 0.0 * inv

    final = alpha * inv + (1.0 - alpha) * sims
    order = np.argsort(-final)

    ranked = []
    for idx in order:
        url, chunk_index, snippet, fts_rank, _chunk_text = rows[idx]
        ranked.append((float(final[idx]), url, chunk_index, snippet, float(fts_rank)))
    return ranked


def main():
    ap = argparse.ArgumentParser(description="Hybrid search (FTS5 + embedding re-rank).")
    ap.add_argument("query", help="FTS5 query (supports AND/OR, wildcards, quotes).")
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    ap.add_argument("--k", type=int, default=25, help="Results to return.")
    ap.add_argument("--alpha", type=float, default=0.55, help="Blend weight: FTS vs embedding.")
    ap.add_argument("--near", help="termA:termB:window (e.g., children:data:25)")
    ap.add_argument("--fetch-mult", type=int, default=5, help="Over-fetch factor before re-rank.")
    args = ap.parse_args()

    near = parse_near(args.near)
    fetch_n = max(args.k * max(1, args.fetch_mult), args.k)

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # NOTE:
    # - No alias inside FTS functions (snippet, rank); use the table name literally.
    # - Use rank (portable) rather than bm25(f) for max compatibility.
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

    rows_tup = [(r["url"], r["chunk_index"], r["snip"], r["fts_rank"], r["chunk_text"]) for r in rows]
    ranked = score_and_rank(rows_tup, args.query, alpha=args.alpha, near=near)

    if not ranked:
        print("No matches.")
        return

    for i, (score, url, idx, snip, fts_rank) in enumerate(ranked[: args.k], start=1):
        print(f"{i:>2}. {url}")
        print(f"    {snip.strip()}")
        print(f"    score={score:.3f}  rank={fts_rank:.3f}")
        print()


if __name__ == "__main__":
    main()
