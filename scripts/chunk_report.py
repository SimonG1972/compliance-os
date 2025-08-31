#!/usr/bin/env python
import sqlite3, os
DB=os.path.join(os.getcwd(),"compliance.db")
con=sqlite3.connect(DB); c=con.cursor()
one=lambda q: c.execute(q).fetchone()[0]
print("docs total      :", one("SELECT COUNT(*) FROM documents"))
print("docs chunked    :", one("SELECT COUNT(DISTINCT doc_id) FROM chunks"))
print("chunks total    :", one("SELECT COUNT(*) FROM chunks"))
print("avg chunk chars :", int(one("SELECT COALESCE(AVG(LENGTH(text)),0) FROM chunks")))
print("\nTop 10 hosts by chunks:")
for h,cnt in c.execute("SELECT host, COUNT(*) FROM chunks GROUP BY host ORDER BY COUNT(*) DESC LIMIT 10"):
    print(f"  {h:30} {cnt}")
print("\nDocs with clean_text but no chunks (sample 10):")
for (u,) in c.execute("""
SELECT url FROM documents
WHERE clean_text IS NOT NULL AND LENGTH(clean_text)>=120
  AND id NOT IN (SELECT DISTINCT doc_id FROM chunks)
LIMIT 10
"""):
    print(" ", u)
con.close()
