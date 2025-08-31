# scripts/init_tags.py
import os, sqlite3
db = os.path.join(os.getcwd(), "compliance.db")
print("Using DB:", db)
con = sqlite3.connect(db)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS chunk_tags (
  id INTEGER PRIMARY KEY,
  chunk_id INTEGER NOT NULL,
  tag TEXT NOT NULL,
  score REAL,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(chunk_id, tag),
  FOREIGN KEY(chunk_id) REFERENCES document_chunks(id)
);
""")
con.commit()
print("OK: chunk_tags table ready.")
