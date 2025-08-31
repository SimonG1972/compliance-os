#!/usr/bin/env python
import os, sqlite3, time

DB = os.path.join(os.getcwd(), "compliance.db")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

-- documents: primary store for fetched pages
CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  url_original TEXT,
  title TEXT,
  body TEXT,
  clean_text TEXT,
  status_code INTEGER,
  render_mode TEXT,
  fetched_at TEXT,
  content_hash TEXT,
  etag TEXT,
  last_modified TEXT,
  last_error TEXT,
  retry_count INTEGER DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_url ON documents(url);

-- discovery queue
CREATE TABLE IF NOT EXISTS discovery_queue (
  url TEXT PRIMARY KEY,
  discovered_from TEXT,
  status TEXT DEFAULT 'pending' -- pending|queued|done|error
);
CREATE INDEX IF NOT EXISTS idx_disc_status ON discovery_queue(status);

-- chunks (base table)
CREATE TABLE IF NOT EXISTS document_chunks (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  chunk_text TEXT NOT NULL,
  token_estimate INTEGER,
  UNIQUE(url, chunk_index)
);
CREATE INDEX IF NOT EXISTS idx_chunks_url ON document_chunks(url);

-- FTS index (contentless)
CREATE VIRTUAL TABLE IF NOT EXISTS document_chunks_fts
USING fts5 (
  url,
  chunk_text,
  tokenize = 'porter'
);

-- simple tag table
CREATE TABLE IF NOT EXISTS chunk_tags (
  chunk_id INTEGER NOT NULL,
  tag TEXT NOT NULL,
  score REAL DEFAULT 1.0,
  UNIQUE(chunk_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_chunk_tags_tag ON chunk_tags(tag);

-- optional revisions (some pipelines use it)
CREATE TABLE IF NOT EXISTS document_revisions (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  fetched_at TEXT,
  content_hash TEXT,
  status_code INTEGER,
  etag TEXT,
  last_modified TEXT,
  render_mode TEXT,
  body TEXT
);
CREATE INDEX IF NOT EXISTS idx_revs_url ON document_revisions(url);
"""

def main():
    if os.path.exists(DB):
        print(f"[info] DB already exists at {DB}")
    else:
        print(f"[info] Creating DB at {DB}")

    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.executescript(SCHEMA)
    con.commit()

    # smoke test
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name"
    ).fetchall()]
    print("[schema] created/ensured tables:", ", ".join(tables))

    # counts should be zero on a fresh DB
    for t in ["documents","document_chunks","document_chunks_fts","chunk_tags","document_revisions","discovery_queue"]:
        try:
            c = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:24s}: {c}")
        except Exception as e:
            print(f"  {t:24s}: (missing)")

    con.close()
    print("[done] bootstrap complete.")

if __name__ == "__main__":
    main()
