import os, sqlite3, random

DB = os.path.join(os.getcwd(), "compliance.db")
print("DB:", DB)
if not os.path.exists(DB):
    raise SystemExit("DB not found")

con = sqlite3.connect(DB)
cur = con.cursor()

# Basic table counts
rows = cur.execute("SELECT COUNT(*) FROM document_chunks_fts").fetchone()[0]
print("document_chunks_fts rows:", rows)

# Peek at one row to confirm there’s readable text
row = cur.execute("SELECT url, substr(chunk_text,1,200) FROM document_chunks_fts LIMIT 1").fetchone()
print("\nSample row url/text:")
print(row)

# Try simple MATCH terms that should exist in a big web corpus
for term in ["privacy", "data", "children", "cookie", "terms"]:
    n_match = cur.execute(
        "SELECT COUNT(*) FROM document_chunks_fts WHERE document_chunks_fts MATCH ?",
        (term,)
    ).fetchone()[0]
    n_like = cur.execute(
        "SELECT COUNT(*) FROM document_chunks_fts WHERE chunk_text LIKE ?",
        (f"%{term}%",)
    ).fetchone()[0]
    print(f"\nterm: {term!r}  MATCH={n_match}  LIKE={n_like}")

# If MATCH=0 but LIKE>0 for a term, the FTS index needs a rebuild.
con.close()
