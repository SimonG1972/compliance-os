# scripts/check_changes.py
import os, sqlite3, argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    ap.add_argument("--since", default="-7 days", help="SQLite relative interval, e.g. '-3 days', '-24 hours'")
    ap.add_argument("--limit", type=int, default=100)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    rows = cur.execute(f"""
      SELECT url, fetched_at, substr(clean_text,1,120)
      FROM document_revisions
      WHERE datetime(fetched_at) >= datetime('now', ?)
      ORDER BY datetime(fetched_at) DESC
      LIMIT ?
    """, (args.since, args.limit)).fetchall()

    if not rows:
        print("No recent changes.")
        return

    for url, ts, preview in rows:
        print(f"- {ts}  {url}")
        print(f"  { (preview or '').replace('\\n',' ') }")
    print(f"\nTotal shown: {len(rows)}")

if __name__ == "__main__":
    main()
