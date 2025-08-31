#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ensure the 'chunks' table exists and has the columns chunk_docs.py expects.
- Adds missing columns: char_len (INTEGER), token_estimate (INTEGER)
- Creates the table if it doesn't exist
- Idempotent: safe to re-run
"""
import os, sqlite3
DB = os.path.join(os.getcwd(), "compliance.db")

def table_info(cur, table):
    try:
        return cur.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return []

def colnames(info_rows):
    return {r[1] for r in info_rows}  # PRAGMA cols: cid, name, type, notnull, dflt, pk

def ensure_column(cur, table, col, coltype):
    if col not in colnames(table_info(cur, table)):
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")

def ensure_table(cur):
    if table_info(cur, "chunks"):  # already exists
        return
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

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    ensure_table(cur)
    # Ensure the two expected columns exist (older DBs may lack them)
    ensure_column(cur, "chunks", "char_len", "INTEGER")
    ensure_column(cur, "chunks", "token_estimate", "INTEGER")
    con.commit()
    con.close()
    print("[ensure_chunk_schema] OK: 'chunks' table ready with char_len and token_estimate.")

if __name__ == "__main__":
    main()
