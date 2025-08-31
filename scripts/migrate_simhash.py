import os, sqlite3, sys

DB = os.path.join(os.getcwd(), "compliance.db")
print("Using DB:", DB)
con = sqlite3.connect(DB)
cur = con.cursor()

# Add simhash_hex TEXT if it doesn't exist
cur.execute("PRAGMA table_info(document_chunks)")
cols = {r[1] for r in cur.fetchall()}
if "simhash_hex" not in cols:
    print("-> Adding column simhash_hex TEXT …")
    cur.execute("ALTER TABLE document_chunks ADD COLUMN simhash_hex TEXT")
else:
    print("-> simhash_hex already exists")

# Optional: fast lookup index (not required, but nice)
cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_chunks_simhash_hex'")
if not cur.fetchone():
    print("-> Creating index idx_chunks_simhash_hex …")
    cur.execute("CREATE INDEX idx_chunks_simhash_hex ON document_chunks(simhash_hex)")
else:
    print("-> index idx_chunks_simhash_hex already exists")

con.commit()
con.close()
print("Done.")
