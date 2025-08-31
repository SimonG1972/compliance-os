# scripts/init_revisions.py
import os, sqlite3
db = os.path.join(os.getcwd(), "compliance.db")
print("Using DB:", db)
con = sqlite3.connect(db)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS document_revisions (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  content_hash TEXT,
  clean_text TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
""")

# When documents.content_hash changes, capture a snapshot row
cur.execute("""
CREATE TRIGGER IF NOT EXISTS trg_documents_revision
AFTER UPDATE OF content_hash ON documents
WHEN OLD.content_hash IS NOT NEW.content_hash
BEGIN
  INSERT INTO document_revisions(url, fetched_at, content_hash, clean_text)
  VALUES (NEW.url, COALESCE(NEW.fetched_at, datetime('now')), NEW.content_hash, NEW.clean_text);
END;
""")

con.commit()
print("OK: document_revisions table + trigger installed.")
