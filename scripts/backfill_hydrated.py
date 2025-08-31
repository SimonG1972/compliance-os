import sqlite3

DB = "compliance.db"
con = sqlite3.connect(DB)
c = con.cursor()

# Mark any queue row as hydrated if we already have a document for its URL
c.execute("""
UPDATE discovery_queue
   SET status = 'hydrated'
 WHERE url IN (SELECT url FROM documents)
""")
con.commit()

tot = c.execute("SELECT COUNT(*) FROM discovery_queue").fetchone()[0]
hyd = c.execute("SELECT COUNT(*) FROM discovery_queue WHERE status='hydrated'").fetchone()[0]
pend = tot - hyd

print("Backfill done.")
print("total:", tot, "hydrated:", hyd, "pending:", pend)

con.close()
