#!/usr/bin/env python
import sqlite3

DB_PATH = "compliance.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("DELETE FROM documents WHERE url LIKE '%pinterest.com%'")
cur.execute("DELETE FROM discovery_queue WHERE url LIKE '%pinterest.com%'")

con.commit()
con.close()

print("✅ Pinterest rows removed")
