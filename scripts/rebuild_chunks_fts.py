# scripts/rebuild_chunks_fts.py
import os, sqlite3
db = os.path.join(os.getcwd(), "compliance.db")
print("Using DB:", db)
con = sqlite3.connect(db)
cur = con.cursor()
cur.execute("INSERT INTO document_chunks_fts(document_chunks_fts) VALUES('rebuild');")
con.commit()
cnt = cur.execute("SELECT count(*) FROM document_chunks_fts").fetchone()[0]
print(f"Rebuilt document_chunks_fts. FTS rows: {cnt}")
# quick smoke for snippet
row = cur.execute("""
SELECT snippet(document_chunks_fts, 0, '[', ']', ' … ', 12)
FROM document_chunks_fts
WHERE document_chunks_fts MATCH 'privacy'
LIMIT 1
""").fetchone()
print("snippet sample:", (row[0][:120] + "…") if row and row[0] else "(none)")
