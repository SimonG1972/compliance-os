#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import math
import argparse
import sqlite3
from collections import defaultdict, Counter
from urllib.parse import urlparse

# Optional embeddings for hybrid re-rank (falls back to FTS-only)
_HAS_ST = True
try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
except Exception:
    _HAS_ST = False

DB_PATH = os.environ.get("COMPLIANCE_DB", os.path.join(os.getcwd(), "compliance.db"))
MODEL_NAME = os.environ.get("EMB_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

_WORD = re.compile(r"\w+", re.UNICODE)

def get_conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def parse_near(near_str):
    if not near_str:
        return None
    try:
        a, b, w = near_str.split(":")
        return a.strip().lower(), b.strip().lower(), int(w)
    except Exception:
        raise SystemExit("Invalid --near. Use: termA:termB:window (e.g., children:data:25)")

def within_window(text, a, b, win):
    toks = [t.lower() for t in _WORD.findall(text)]
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

def fts_top(query, fetch_n):
    con = get_conn()
    cur = con.cursor()
    # IMPORTANT: join FTS rowid to concrete table id
    rows = cur.execute(
        """
        WITH fts_hits AS (
          SELECT
            document_chunks_fts.rowid AS rid,
            rank                       AS fts_rank,
            snippet(document_chunks_fts, 1, '[', ']', ' … ', 12) AS snip
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
        (query, fetch_n),
    ).fetchall()
    con.close()
    # return tuples for downstream: (url, chunk_index, snippet, fts_rank, chunk_text)
    return [(r["url"], r["chunk_index"], r["snip"], float(r["fts_rank"]), r["chunk_text"]) for r in rows]

def hybrid_rank(rows, query, alpha=0.55, near=None):
    # rows: (url, chunk_index, snippet, fts_rank, chunk_text)
    if near:
        a, b, w = near
        rows = [r for r in rows if within_window(r[4], a, b, w)]
    if not rows:
        return []

    # Normalize inverse FTS rank
    import numpy as np
    fts = np.array([r[3] for r in rows], dtype=float)
    fts = np.where(np.isnan(fts), np.nanmedian(fts), fts)
    inv = -fts
    inv -= np.min(inv)
    if np.max(inv) > 0:
        inv /= np.max(inv)

    if _HAS_ST:
        model = SentenceTransformer(MODEL_NAME)
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

    final = alpha * inv + (1.0 - alpha) * sims
    order = np.argsort(-final)
    ranked = [rows[i] + (float(final[i]),) for i in order]  # append score
    return ranked

def split_sentences(text):
    # light sentence splitter; avoids pulling CSS/JS leftovers (already filtered in your pipeline)
    text = re.sub(r"\s+", " ", text).strip()
    # keep periods/question/exclamation as delimiters
    parts = re.split(r"(?<=[\.\?\!])\s+(?=[A-Z0-9“\"'])", text)
    # filter too short or too long
    return [p.strip() for p in parts if 30 <= len(p.strip()) <= 600]

def score_sentences(sentences, query_tokens):
    scores = []
    for s in sentences:
        toks = [t.lower() for t in _WORD.findall(s)]
        if not toks:
            scores.append(0.0)
            continue
        overlap = len(set(toks) & query_tokens)
        # small tf bonus
        tf = sum(1 for t in toks if t in query_tokens)
        # length normalization
        ln = 1.0 / math.log2(10 + len(toks))
        scores.append((overlap * 2 + tf) * ln)
    return scores

def synthesize_answer(q, items, max_sentences=6, per_host_cap=2):
    """
    items: list of tuples (url, chunk_index, snippet, fts_rank, chunk_text, score)
    returns (markdown, sources_list)
    """
    # prepare sentences pool
    query_tokens = set(t.lower() for t in _WORD.findall(q))
    candidates = []  # (score, sentence, url, chunk_index)
    for (url, idx, _sn, _fr, chunk, _score) in items:
        sents = split_sentences(chunk)
        if not sents:
            continue
        sscores = score_sentences(sents, query_tokens)
        for s, sc in zip(sents, sscores):
            if sc > 0:
                candidates.append((sc, s, url, idx))

    if not candidates:
        return ("No high-signal sentences found for this query.", [])

    # sort by score desc and enforce per_host cap
    candidates.sort(key=lambda x: -x[0])
    taken = []
    host_count = defaultdict(int)
    for sc, s, url, idx in candidates:
        host = urlparse(url).netloc or "unknown"
        if host_count[host] >= per_host_cap:
            continue
        taken.append((s, url, idx))
        host_count[host] += 1
        if len(taken) >= max_sentences:
            break

    # build citation map
    src_index = {}
    sources = []
    def cite(url):
        if url not in src_index:
            src_index[url] = len(sources) + 1
            sources.append(url)
        return src_index[url]

    # assemble markdown
    bullets = []
    for s, url, idx in taken:
        n = cite(url)
        bullets.append(f"- {s} [^{n}]")

    md = []
    md.append(f"## Answer: {q}\n")
    md.append("\n".join(bullets))
    md.append("\n## Sources\n")
    for i, u in enumerate(sources, start=1):
        md.append(f"[^{i}]: {u}")
    return ("\n".join(md).strip(), sources)

def main():
    ap = argparse.ArgumentParser(description="Synthesize a sourced answer from top passages.")
    ap.add_argument("query", help="FTS5 query, e.g. \"children data retention\" or \"children AND data\"")
    ap.add_argument("--k", type=int, default=40, help="How many ranked chunks to consider")
    ap.add_argument("--near", type=str, default=None, help="Proximity guardrail termA:termB:window")
    ap.add_argument("--alpha", type=float, default=0.55, help="FTS vs embedding mix (0..1)")
    ap.add_argument("--fetch-mult", type=int, default=5, help="Over-fetch multiple before re-rank")
    ap.add_argument("--out", type=str, default=None, help="Write Markdown to this path")
    args = ap.parse_args()

    fetch_n = max(args.k * max(1, args.fetch_mult), args.k)
    near_tuple = parse_near(args.near)

    rows = fts_top(args.query, fetch_n)
    ranked = hybrid_rank(rows, args.query, alpha=args.alpha, near=near_tuple)[: args.k]

    if not ranked:
        print("No results for that query.")
        sys.exit(0)

    md, sources = synthesize_answer(args.query, ranked, max_sentences=6, per_host_cap=2)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"Wrote {args.out} (sources: {len(sources)})")
    else:
        print(md)

if __name__ == "__main__":
    main()
