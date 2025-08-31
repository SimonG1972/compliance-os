import os, sqlite3

DB = os.path.join(os.getcwd(), "compliance.db")
print("DB:", DB)
if not os.path.exists(DB):
    raise SystemExit("DB not found")

con = sqlite3.connect(DB)
cur = con.cursor()

print("Before:", cur.execute("SELECT COUNT(*) FROM document_chunks_fts").fetchone()[0], "rows")
print("Rebuilding FTS index…")
cur.execute("INSERT INTO document_chunks_fts(document_chunks_fts) VALUES('rebuild')")
con.commit()
print("Optimizing FTS index…")
cur.execute("INSERT INTO document_chunks_fts(document_chunks_fts) VALUES('optimize')")
con.commit()

print("After:", cur.execute("SELECT COUNT(*) FROM document_chunks_fts").fetchone()[0], "rows")
con.close()
print("Done.")
