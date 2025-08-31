import os, sqlite3, sys

DB = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "compliance.db")
print("DB:", DB)

con = sqlite3.connect(DB)
cur = con.cursor()

# integrity
status = cur.execute("PRAGMA integrity_check;").fetchone()[0]
print("integrity_check:", status)

# show row counts if possible
def count(table):
    try:
        return cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception as e:
        return f"ERR({e.__class__.__name__})"

for t in ["documents", "document_chunks", "document_chunks_fts", "chunk_tags", "document_revisions"]:
    print(f"{t}: {count(t)}")

con.close()
