#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
queue_counts.py
- Quick counts for discovery_queue and documents, plus a small pending sample
"""

import os, sqlite3, textwrap

DB = os.path.join(os.getcwd(), "compliance.db")

def table_exists(cur, name):
    try:
        r = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        return bool(r)
    except Exception:
        return False

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # discovery_queue
    dq_tot = dq_hyd = dq_err = dq_pending = 0
    if table_exists(cur, "discovery_queue"):
        dq_tot = cur.execute("SELECT COUNT(*) FROM discovery_queue").fetchone()[0]
        dq_hyd = cur.execute("SELECT COUNT(*) FROM discovery_queue WHERE status='hydrated'").fetchone()[0]
        dq_err = cur.execute("SELECT COUNT(*) FROM discovery_queue WHERE status='error'").fetchone()[0]
        dq_pending = dq_tot - dq_hyd

    # documents (robust to missing columns)
    docs_tot = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    docs_ok = cur.execute("SELECT COUNT(*) FROM documents WHERE COALESCE(status_code,0) IN (200,304) AND LENGTH(COALESCE(body,''))>0").fetchone()[0]
    docs_clean = cur.execute("SELECT COUNT(*) FROM documents WHERE LENGTH(COALESCE(clean_text,''))>0").fetchone()[0]
    docs_clean_120 = cur.execute("SELECT COUNT(*) FROM documents WHERE LENGTH(COALESCE(clean_text,''))>=120").fetchone()[0]

    chunks_tot = 0
    if table_exists(cur, "chunks"):
        chunks_tot = cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]

    print("\n=== discovery_queue ===")
    print(f" total    : {dq_tot}")
    print(f" hydrated : {dq_hyd}")
    print(f" error    : {dq_err}")
    print(f" pending  : {dq_pending}")

    print("\n=== documents ===")
    print(f" total           : {docs_tot}")
    print(f" fetched_ok      : {docs_ok}")
    print(f" clean_text>0    : {docs_clean}")
    print(f" clean_text>=120 : {docs_clean_120}")

    print("\n=== chunks ===")
    print(f" total_chunks : {chunks_tot}")

    # show a small sample of pending queue (if any)
    if dq_pending > 0 and table_exists(cur, "discovery_queue"):
        print("\n=== sample pending (up to 10) ===")
        # If your schema lacks timestamps, this still works—just returns any 10 not hydrated
        rows = cur.execute("""
            SELECT url, COALESCE(discovered_from,'') as src
              FROM discovery_queue
             WHERE COALESCE(status,'') <> 'hydrated'
             LIMIT 10
        """).fetchall()
        for i,(u,src) in enumerate(rows,1):
            print(f" {i:2}. {u}    [{src}]")

    con.close()
    print("\n[queue_counts] done.")

if __name__ == "__main__":
    main()
