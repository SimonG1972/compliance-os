# scripts/hybrid_counts.py
import argparse
import os
import sqlite3
from urllib.parse import urlparse
from collections import Counter

def main():
    ap = argparse.ArgumentParser(description="Count hosts for a hybrid FTS slice (FTS rank only).")
    ap.add_argument("query")
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    ap.add_argument("--limit", type=int, default=5000)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT c.url
        FROM document_chunks_fts
        JOIN document_chunks c
          ON c.id = document_chunks_fts.rowid
        WHERE document_chunks_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (args.query, args.limit),
    ).fetchall()
    con.close()

    hosts = [urlparse(r["url"]).netloc or "unknown" for r in rows]
    cnt = Counter(hosts).most_common()
    if not cnt:
        print("No matches.")
        return
    for host, n in cnt:
        print(f"{host}: {n}")

if __name__ == "__main__":
    main()
