# scripts/simulate_changes.py
import os, sqlite3, argparse
from datetime import datetime, timezone

DB_DEFAULT = os.path.join(os.getcwd(), "compliance.db")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--limit", type=int, default=5, help="How many URLs to simulate updates for")
    ap.add_argument("--like", default="", help="Optional filter: only URLs LIKE this (e.g. '%%snap.com%%')")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    where_like = ""
    params = []
    if args.like:
        where_like = "AND url LIKE ?"
        params.append(args.like)

    # Pick rows that look healthy (status_code=200) and have some body/clean_text
    rows = cur.execute(f"""
        SELECT url, COALESCE(content_hash,''), COALESCE(clean_text, body, '')
        FROM documents
        WHERE status_code = 200
          AND COALESCE(body,'') <> ''
          {where_like}
        ORDER BY fetched_at DESC
        LIMIT ?
    """, (*params, args.limit)).fetchall()

    if not rows:
        print("No candidate rows found (status_code=200 with content). Try adjusting --like or --limit.")
        return

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    updated = 0
    for url, old_hash, text in rows:
        # create a new hash string to ensure the trigger condition OLD.content_hash != NEW.content_hash
        new_hash = (old_hash or "seed") + f":sim@{now_iso}"
        # nudge the clean_text so the revision contains before/after text
        new_text = (text or "") + f"\n\n(automated simulation at {now_iso})"

        cur.execute("""
            UPDATE documents
            SET content_hash = ?,
                clean_text   = ?,
                fetched_at   = ?
            WHERE url = ?
        """, (new_hash, new_text, now_iso, url))
        updated += 1

    con.commit()
    print(f"Simulated updates on {updated} URLs.")
    # Show last few revision rows for sanity
    for r in cur.execute("""
        SELECT url, substr(prev_hash,1,24)||'…', substr(new_hash,1,24)||'…', changed_at, change_kind
        FROM document_revisions
        ORDER BY datetime(COALESCE(changed_at,'')) DESC, id DESC
        LIMIT 5
    """).fetchall():
        print("Δ", r)

    con.close()

if __name__ == "__main__":
    main()
