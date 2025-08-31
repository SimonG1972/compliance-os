# scripts/peek_null_clean.py
import sqlite3, urllib.parse, collections, sys

con = sqlite3.connect("compliance.db")
cur = con.cursor()

# Show sample rows for a specific host (default instagram.com)
host = sys.argv[1] if len(sys.argv) > 1 else "www.instagram.com"
q = """
SELECT id, url, LENGTH(body)
FROM documents
WHERE url LIKE ? AND (clean_text IS NULL OR LENGTH(clean_text)=0)
ORDER BY LENGTH(body) DESC LIMIT 10
"""
rows = cur.execute(q, (f"https://{host}/%",)).fetchall()
print(f"\nTop 10 NULL clean_text rows for {host}:\n")
for r in rows:
    print(r)

# Summary top 20 hosts by NULL clean_text
urls = cur.execute("SELECT url FROM documents WHERE clean_text IS NULL OR LENGTH(clean_text)=0").fetchall()
host_counts = collections.Counter(urllib.parse.urlparse(u).netloc for (u,) in urls)
print("\nTop 20 hosts with NULL/empty clean_text:\n")
for h, n in host_counts.most_common(20):
    print(f"{h:30} {n}")

con.close()
