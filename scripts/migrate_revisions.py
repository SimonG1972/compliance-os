# scripts/migrate_revisions.py
import os, sqlite3, argparse, sys
from datetime import datetime

DB = os.path.join(os.getcwd(), "compliance.db")

ADD_COLS_SQL = [
    ("prev_hash",  "TEXT"),
    ("new_hash",   "TEXT"),
    ("prev_text",  "TEXT"),
    ("new_text",   "TEXT"),
    ("changed_at", "TEXT"),
    ("change_kind","TEXT"),
]

TRIGGER_SQL = r"""
CREATE TRIGGER IF NOT EXISTS trg_documents_change_capture
AFTER UPDATE OF content_hash, clean_text, body ON documents
WHEN COALESCE(OLD.content_hash,'') != COALESCE(NEW.content_hash,'')
BEGIN
  INSERT INTO document_revisions
    (url, fetched_at, content_hash, clean_text, created_at,
     prev_hash, new_hash, prev_text, new_text, changed_at, change_kind)
  VALUES
    (NEW.url,
     NEW.fetched_at,
     NEW.content_hash,
     COALESCE(NEW.clean_text, NEW.body, ''),
     datetime('now'),
     OLD.content_hash,
     NEW.content_hash,
     COALESCE(OLD.clean_text, OLD.body, ''),
     COALESCE(NEW.clean_text, NEW.body, ''),
     datetime('now'),
     'content_hash_changed');
END;
"""

def table_exists(cur, name):
    return cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None

def ensure_columns(cur):
    cur.execute("PRAGMA table_info(document_revisions)")
    present = {row[1] for row in cur.fetchall()}
    for col, typ in ADD_COLS_SQL:
        if col not in present:
            cur.execute(f"ALTER TABLE document_revisions ADD COLUMN {col} {typ}")

def ensure_trigger(cur):
    # Drop a legacy trigger if needed (optional, defensive)
    # cur.execute("DROP TRIGGER IF EXISTS trg_documents_change_capture")
    cur.execute(TRIGGER_SQL)

def backfill(con):
    cur = con.cursor()
    # We’ll walk rows by URL, ordered by created_at, and fill prev/new for pairs where changed.
    # Only touch rows that don't already have prev/new filled.
    urls = [u for (u,) in cur.execute("SELECT DISTINCT url FROM document_revisions")]
    total_pairs = 0
    for url in urls:
        rows = cur.execute("""
            SELECT id, content_hash, clean_text, created_at
            FROM document_revisions
            WHERE url=?
            ORDER BY datetime(COALESCE(created_at,'')) ASC, id ASC
        """, (url,)).fetchall()
        # rows: (id, content_hash, clean_text, created_at)
        for i in range(1, len(rows)):
            prev = rows[i-1]
            curr = rows[i]
            prev_id, prev_hash, prev_text, prev_ts = prev
            curr_id, curr_hash, curr_text, curr_ts = curr

            if (prev_hash or "") != (curr_hash or ""):
                # Only update if not already filled
                cur.execute("""
                    UPDATE document_revisions
                    SET prev_hash = COALESCE(prev_hash, ?),
                        new_hash  = COALESCE(new_hash,  ?),
                        prev_text = COALESCE(prev_text, ?),
                        new_text  = COALESCE(new_text,  ?),
                        changed_at= COALESCE(changed_at, COALESCE(?, datetime('now'))),
                        change_kind = COALESCE(change_kind, 'backfill')
                    WHERE id = ?
                """, (prev_hash, curr_hash, prev_text or "", curr_text or "", curr_ts, curr_id))
                total_pairs += 1
    return total_pairs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--backfill", action="store_true", help="Backfill prev/new columns from existing rows")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    if not table_exists(cur, "document_revisions"):
        print("❌ document_revisions does not exist. Run your original init first.")
        sys.exit(1)

    # 1) Add missing columns (no-op if already present)
    ensure_columns(cur)
    # 2) Install trigger for future changes
    ensure_trigger(cur)
    con.commit()

    if args.backfill:
        pairs = backfill(con)
        con.commit()
        print(f"Backfill: filled {pairs} change pairs.")

    print("✅ Migration complete: columns ensured + trigger installed.")
    con.close()

if __name__ == "__main__":
    main()
