#!/usr/bin/env python3
import argparse, sqlite3, time
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "compliance.db"

def main():
    ap = argparse.ArgumentParser(description="Seed URLs directly into discovery_queue")
    ap.add_argument("category", help="Category label (e.g. social-tier-4)")
    ap.add_argument("urls", nargs="+", help="One or more URLs to insert")
    ap.add_argument("--apply", action="store_true", help="Actually insert (default is dry-run)")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()

    inserted = 0
    for url in args.urls:
        now = int(time.time())
        cur.execute("""
            SELECT 1 FROM discovery_queue WHERE url = ?
        """, (url,))
        if cur.fetchone():
            print(f"[skip] already queued: {url}")
            continue

        print(f"[queue] {url}  (category={args.category})")
        if args.apply:
            cur.execute("""
                INSERT INTO discovery_queue (url, category, added_at, status)
                VALUES (?, ?, ?, 'pending')
            """, (url, args.category, now))
            inserted += 1

    if args.apply:
        con.commit()
        print(f"[done] inserted {inserted} rows")
    else:
        print(f"[dry-run] would insert {inserted} rows")

if __name__ == "__main__":
    main()
