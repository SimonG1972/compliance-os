#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Promote URLs from discovery_queue into documents so hydrator can fetch them.
Idempotent. Does NOT overwrite existing document rows.
"""
import os, sqlite3

DB = os.path.join(os.getcwd(), "compliance.db")

def main():
    con = sqlite3.connect(DB); c = con.cursor()
    urls = [r[0] for r in c.execute("SELECT url FROM discovery_queue").fetchall()]
    ins = 0
    for u in urls:
        c.execute("""
            INSERT OR IGNORE INTO documents
              (url, url_original, title, body, clean_text, status_code, render_mode)
            VALUES (?,?,?,?,?,?,?)
        """, (u, u, "", "", "", None, "queued"))
        if c.rowcount:
            ins += 1
    con.commit(); con.close()
    print(f"[queue_promote] inserted {ins} new rows into documents from discovery_queue")

if __name__ == "__main__":
    main()
