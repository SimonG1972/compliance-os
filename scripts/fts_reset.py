#!/usr/bin/env python
# scripts/fts_reset.py
import os, sqlite3

DB = os.path.join(os.getcwd(), "compliance.db")

DDL = """
DROP TABLE IF EXISTS document_chunks_fts;
DROP TABLE IF EXISTS document_chunks_fts_data;
DROP TABLE IF EXISTS document_chunks_fts_idx;
DROP TABLE IF EXISTS document_chunks_fts_docsize;
DROP TABLE IF EXISTS document_chunks_fts_config;
DROP TABLE IF EXISTS document_chunks_fts_content;

CREATE VIRTUAL TABLE document_chunks_fts
USING fts5 (
  url,
  chunk_text,
  tokenize = 'porter'
);
"""

con = sqlite3.connect(DB)
cur = con.cursor()
cur.executescript(DDL)
con.commit()
con.close()
print("[fts_reset] document_chunks_fts dropped & recreated empty.")
