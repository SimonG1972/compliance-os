#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
clean_noise_urls.py
Delete obvious noise URLs from documents & discovery_queue:
 - giphy.com search/explore/gifs/stickers
 - trailing backslashes
 - deviantart.com gallery/favourites/etc.
 - vimeo.com gitbook helper paths
"""

import sqlite3
import os

DB_PATH = os.path.join(os.getcwd(), "compliance.db")

NOISE_PATTERNS = [
    "%giphy.com/search/%",
    "%giphy.com/explore/%",
    "%giphy.com/gifs/%",
    "%giphy.com/stickers/%",
    "%deviantart.com/%/gallery%",
    "%deviantart.com/%/favourites%",
    "%vimeo.com/legal/~gitbook%",
    "%\\%",   # trailing backslash cases
]

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    total_deleted = 0

    for pat in NOISE_PATTERNS:
        cur.execute("DELETE FROM documents WHERE url LIKE ?", (pat,))
        cur.execute("DELETE FROM discovery_queue WHERE url LIKE ?", (pat,))
        total_deleted += cur.rowcount
        print(f"[clean] pattern {pat} → {cur.rowcount} rows deleted")

    con.commit()
    con.close()
    print(f"\n[done] Cleanup complete. Total deleted ~ {total_deleted} rows.")

if __name__ == "__main__":
    main()
