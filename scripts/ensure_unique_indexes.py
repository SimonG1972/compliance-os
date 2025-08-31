import sqlite3
con=sqlite3.connect("compliance.db")
c=con.cursor()
c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_url_unique ON documents(url)")
c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_discovery_queue_url_unique ON discovery_queue(url)")
con.commit(); con.close()
print("unique indexes ensured")
