# scripts/init_chunks.py
import os, sqlite3

db = os.path.join(os.getcwd(), "compliance.db")
print("Using DB:", db)
con = sqlite3.connect(db)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS document_chunks (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  chunk_text TEXT NOT NULL,
  token_estimate INTEGER,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(url, chunk_index)
);
""")

# External content FTS bound to document_chunks (rowid=id)
cur.execute("""
CREATE VIRTUAL TABLE IF NOT EXISTS document_chunks_fts
USING fts5(
  chunk_text,
  content='document_chunks',
  content_rowid='id'
);
""")

# Triggers to keep FTS in sync
cur.execute("""
CREATE TRIGGER IF NOT EXISTS document_chunks_ai
AFTER INSERT ON document_chunks BEGIN
  INSERT INTO document_chunks_fts(rowid, chunk_text)
  VALUES (new.id, new.chunk_text);
END;
""")
cur.execute("""
CREATE TRIGGER IF NOT EXISTS document_chunks_ad
AFTER DELETE ON document_chunks BEGIN
  INSERT INTO document_chunks_fts(document_chunks_fts, rowid, chunk_text)
  VALUES('delete', old.id, old.chunk_text);
END;
""")
cur.execute("""
CREATE TRIGGER IF NOT EXISTS document_chunks_au
AFTER UPDATE OF chunk_text ON document_chunks BEGIN
  INSERT INTO document_chunks_fts(document_chunks_fts, rowid, chunk_text)
  VALUES('delete', old.id, old.chunk_text);
  INSERT INTO document_chunks_fts(rowid, chunk_text)
  VALUES (new.id, new.chunk_text);
END;
""")

con.commit()
print("OK: document_chunks + document_chunks_fts + triggers are ready.")
