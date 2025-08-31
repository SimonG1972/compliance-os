#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Show current chunk inventory:
- totals
- docs without chunks
- top hosts by chunk count
"""

import sqlite3

DB_PATH = "compliance.db"

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT COUNT(*), IFNULL(SUM(char_count),0) FROM chunks")
    total_chunks, total_chars = cur.fetchone()

    cur.execute("SELECT COUNT(DISTINCT doc_id) FROM chunks")
    docs_with_chunks = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM documents WHERE body IS NOT NULL AND TRIM(body) <> ''")
    hydrated_docs = cur.fetchone()[0]

    cur.execute("""
        SELECT substr(url, instr(url, '://')+3, instr(substr(url, instr(url, '://')+3), '/')-1) AS host,
               COUNT(*)
        FROM chunks
        GROUP BY host
        ORDER BY COUNT(*) DESC
        LIMIT 25
    """)
    top = cur.fetchall()

    print("chunks status")
    print("==============================================")
    print(f"total chunks:        {total_chunks}")
    print(f"total chunk chars:   {total_chars:,}")
    avg = (total_chars // total_chunks) if total_chunks else 0
    print(f"avg chunk length:    {avg} chars")
    print(f"hydrated docs:       {hydrated_docs}")
    print(f"docs with chunks:    {docs_with_chunks}")
    print(f"docs lacking chunks: {max(0, hydrated_docs - docs_with_chunks)}")
    print("\nTop hosts by chunks:")
    print("host                               chunks")
    print("---------------------------------  ------")
    for h, c in top:
        print(f"{(h or '-'):33}  {c:6}")
    print("==============================================")

    con.close()

if __name__ == "__main__":
    main()
