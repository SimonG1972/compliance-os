#!/usr/bin/env python
import os, sqlite3
DB = os.path.join(os.getcwd(), "compliance.db")
con = sqlite3.connect(DB)
cur = con.cursor()
def cnt(sql):
    try: return cur.execute(sql).fetchone()[0]
    except: return -1
docs = cnt("SELECT COUNT(*) FROM documents")
pending = cnt("SELECT COUNT(*) FROM documents WHERE COALESCE(body,'')=''")
queued = cnt("SELECT COUNT(*) FROM discovery_queue WHERE status='queued'")
print(f"documents_total: {docs}")
print(f"documents_pending_hydration: {pending}")
print(f"discovery_queue_queued: {queued}")
con.close()
