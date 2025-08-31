#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
DB Sanity Check
- Quick scans for pre-policy cruft:
  * YouTube ?hl= / override_hl= variants
  * Vimeo blog pages
  * LinkedIn oddball hosts
  * Generic querystrings that look like trackers
"""

import sqlite3, os

DB = os.path.join(os.getcwd(), "compliance.db")

CHECKS = [
    # YouTube locale cruft
    ("YouTube ?hl= params",
     "SELECT url FROM documents WHERE url LIKE '%youtube.com%' AND url LIKE '%?hl=%' LIMIT 20"),
    ("YouTube override_hl params",
     "SELECT url FROM documents WHERE url LIKE '%youtube.com%' AND url LIKE '%override_hl=%' LIMIT 20"),
    # Vimeo blog
    ("Vimeo blog pages",
     "SELECT url FROM documents WHERE url LIKE '%vimeo.com/blog%' LIMIT 20"),
    # LinkedIn oddball
    ("LinkedIn help/legal",
     "SELECT url FROM documents WHERE url LIKE '%linkedin.com%' AND url NOT LIKE '%/legal%' LIMIT 20"),
    # Tracker params
    ("Tracker params (utm/fbclid/gclid)",
     "SELECT url FROM documents WHERE url LIKE '%utm_%' OR url LIKE '%fbclid%' OR url LIKE '%gclid%' LIMIT 20"),
]

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    for name, sql in CHECKS:
        print(f"\n=== {name} ===")
        try:
            rows = cur.execute(sql).fetchall()
            if not rows:
                print("  (none)")
            else:
                for r in rows:
                    print(" ", r[0])
        except Exception as e:
            print("  ERROR:", e)
    con.close()

if __name__ == "__main__":
    main()
