#!/usr/bin/env python
import os, sqlite3, sys

DB = os.path.join(os.getcwd(), "compliance.db")
print("Using DB:", DB)
con = sqlite3.connect(DB)
cur = con.cursor()

# 0.1 document_chunks quality columns (safe if exist)
for sql in [
    "ALTER TABLE document_chunks ADD COLUMN signal_score REAL",
    "ALTER TABLE document_chunks ADD COLUMN simhash INTEGER",
]:
    try:
        cur.execute(sql)
        print("OK:", sql)
    except sqlite3.OperationalError:
        pass

# 0.2 fetch metadata on documents (for caching & diff)
for sql in [
    "ALTER TABLE documents ADD COLUMN etag TEXT",
    "ALTER TABLE documents ADD COLUMN last_modified TEXT",
]:
    try:
        cur.execute(sql)
        print("OK:", sql)
    except sqlite3.OperationalError:
        pass

# 0.3 change events table (semantic diff records)
cur.execute("""
CREATE TABLE IF NOT EXISTS change_events (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  changed_at TEXT NOT NULL,
  prev_hash TEXT,
  new_hash TEXT,
  diff_summary TEXT,
  reason_code TEXT,
  tag_hints TEXT,
  revision INTEGER DEFAULT 0
)
""")
print("OK: change_events")

# 0.4 discovery queue (focused crawler enqueue)
cur.execute("""
CREATE TABLE IF NOT EXISTS discovery_queue (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  discovered_from TEXT,
  status TEXT DEFAULT 'pending',
  created_at TEXT DEFAULT (datetime('now')),
  last_attempt_at TEXT
)
""")
print("OK: discovery_queue")

# 0.5 host boilerplate registry (throttle common footers)
cur.execute("""
CREATE TABLE IF NOT EXISTS host_boilerplate (
  host TEXT PRIMARY KEY,
  phrases TEXT  -- JSON array of phrases to suppress
)
""")
print("OK: host_boilerplate")

con.commit()
con.close()
print("Done.")
