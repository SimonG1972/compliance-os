#!/usr/bin/env python
import os, sqlite3, argparse, time

DB = os.path.join(os.getcwd(), "compliance.db")

TABLES = [
    "discovery_queue",
    "document_revisions",
    "chunk_tags",
    "document_chunks",
    "documents",
]

def table_exists(cur, name):
    try:
        row = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE (type='table' OR type='view') AND name=?",
            (name,)
        ).fetchone()
        return bool(row)
    except Exception:
        return False

def count_or_none(cur, name):
    try:
        return cur.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser(description="Wipe content rows (NOT schema) for a fresh crawl.")
    ap.add_argument("--db", default=DB, help="Path to compliance.db")
    ap.add_argument("--confirm", required=True, choices=["yes"], help='Must pass "--confirm yes" to run')
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"[ERR] DB not found: {args.db}")
        return

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    # Show counts before
    print(f"[info] Using DB: {args.db}")
    before = {}
    for t in TABLES:
        if table_exists(cur, t):
            before[t] = count_or_none(cur, t)
        else:
            before[t] = "(missing)"
    # FTS count (if present)
    fts_present = table_exists(cur, "document_chunks_fts")
    fts_before = None
    if fts_present:
        fts_before = count_or_none(cur, "document_chunks_fts")

    print("[before]")
    for k,v in before.items():
        print(f"  {k:24s}: {v}")
    if fts_present:
        print(f"  {'document_chunks_fts':24s}: {fts_before}")

    # Wipe in a transaction
    cur.execute("PRAGMA foreign_keys = OFF;")
    con.commit()
    cur.execute("BEGIN;")
    try:
        for t in TABLES:
            if table_exists(cur, t):
                cur.execute(f"DELETE FROM {t};")
        con.commit()
    except Exception as e:
        con.rollback()
        print(f"[ERR] Delete failed: {e}")
        con.close()
        return

    # Clear FTS using the recommended 'delete-all' if possible
    if fts_present:
        try:
            cur.execute("INSERT INTO document_chunks_fts(document_chunks_fts) VALUES('delete-all');")
            con.commit()
        except Exception:
            # fallback
            try:
                cur.execute("DELETE FROM document_chunks_fts;")
                con.commit()
            except Exception as e:
                print(f"[WARN] Could not clear FTS: {e}")

    # Maintenance
    try:
        cur.execute("VACUUM;")
        cur.execute("REINDEX;")
        con.commit()
    except Exception:
        pass

    # Show counts after
    after = {}
    for t in TABLES:
        if table_exists(cur, t):
            after[t] = count_or_none(cur, t)
        else:
            after[t] = "(missing)"
    fts_after = None
    if fts_present:
        fts_after = count_or_none(cur, "document_chunks_fts")

    print("[after]")
    for k,v in after.items():
        print(f"  {k:24s}: {v}")
    if fts_present:
        print(f"  {'document_chunks_fts':24s}: {fts_after}")

    print("[done] Content wiped. Schema preserved.")

    con.close()

if __name__ == "__main__":
    main()
