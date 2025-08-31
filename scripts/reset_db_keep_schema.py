#!/usr/bin/env python
import os, sqlite3, argparse, shutil, time

ROOT = os.getcwd()
DB = os.path.join(ROOT, "compliance.db")
BACKUPS = os.path.join(ROOT, "backups")

TABLES = [
    "documents",
    "document_chunks",
    "chunk_tags",
    "document_revisions",
    "discovery_queue"
]

def backup_db():
    os.makedirs(BACKUPS, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    dst = os.path.join(BACKUPS, f"compliance-{ts}.db")
    shutil.copy2(DB, dst)
    return dst

def main():
    ap = argparse.ArgumentParser(description="Soft-reset data while keeping schema.")
    ap.add_argument("--yes", action="store_true", help="proceed without interactive prompt")
    args = ap.parse_args()

    if not os.path.exists(DB):
        print("DB not found:", DB)
        return

    if not args.yes:
        print("This will DELETE data from key tables but keep schema.")
        print("A timestamped backup will be created first.")
        resp = input("Type 'RESET' to continue: ").strip()
        if resp != "RESET":
            print("Aborted.")
            return

    bpath = backup_db()
    print("Backup created:", bpath)

    con = sqlite3.connect(DB)
    cur = con.cursor()

    # truncate base tables
    for t in TABLES:
        try:
            cur.execute(f"DELETE FROM {t}")
            print("Cleared", t)
        except Exception as e:
            print("Skip/err clearing", t, e)

    # clear FTS content
    try:
        cur.execute("INSERT INTO document_chunks_fts(document_chunks_fts) VALUES('delete-all')")
        print("Cleared FTS: document_chunks_fts")
    except Exception as e:
        print("FTS clear failed (ok if table absent):", e)

    con.commit()
    try:
        cur.execute("VACUUM")
    except Exception:
        pass
    con.close()
    print("Done. DB reset complete (schema preserved).")

if __name__ == "__main__":
    main()
