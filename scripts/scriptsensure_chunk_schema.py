#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ensure the 'chunks' table exists and has the columns chunk_docs.py expects.
- Adds missing columns: char_len (INTEGER), token_estimate (INTEGER)
- Creates the table if it doesn't exist (safe defaults)
- Idempotent: re-running is safe
"""

import os, sqlite3

DB = os.path.join(os.getcwd(), "compliance.db")

def table_info(cur, table):
    try:
        return cur.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return []

def colnames(info_rows):
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    return {r[1] for r in info_rows}

def ensure_column(cur, table, col, coltype):
    info = table_info(cur, table)
    names = colnames(info)
    if col not in names:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")

def ensure_table(cur):
    # does table exist?
    info = table_info(cur, "chunks")
    if info:
        return  # already exists
    # create with a sensible schema expected by chunk_docs.py
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
    # light index for lookups (optional, safe if it already exists)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON chunks(doc_id)")
    except Exception:
        pass

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # ensure table exists
    ensure_table(cur)

    # ensure required columns exist
    ensure_column(cur, "chunks", "char_len", "INTEGER")
    ensure_column(cur, "chunks", "token_estimate", "INTEGER")

    con.commit()
    con.close()
    print("[ensure_chunk_schema] OK: 'chunks' table ready with char_len and token_estimate.")

if __name__ == "__main__":
    main()
