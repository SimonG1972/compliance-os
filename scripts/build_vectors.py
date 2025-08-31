#!/usr/bin/env python
import os, sqlite3, argparse, sys
DB = os.path.join(os.getcwd(), "compliance.db")

def main():
    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except Exception:
        print("sentence-transformers not installed; skipping vector build.")
        return
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200000)
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()
    rows = cur.execute("SELECT rowid, chunk_text FROM document_chunks LIMIT ?", (args.limit,)).fetchall()
    texts = [t for _, t in rows]
    model = SentenceTransformer("all-MiniLM-L6-v2")
    embs = model.encode(texts, batch_size=256, show_progress_bar=True, convert_to_numpy=True)
    # store in a side table (rowid -> blob)
    cur.execute("CREATE TABLE IF NOT EXISTS chunk_vectors (rowid INTEGER PRIMARY KEY, vec BLOB)")
    cur.execute("DELETE FROM chunk_vectors")
    for (rid, _), v in zip(rows, embs):
        cur.execute("INSERT OR REPLACE INTO chunk_vectors(rowid, vec) VALUES (?, ?)", (rid, v.tobytes()))
    con.commit(); con.close()
    print("Vector build complete.")

if __name__ == "__main__":
    main()
