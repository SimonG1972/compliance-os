#!/usr/bin/env python
import os, sys, sqlite3, argparse, shutil, time, traceback

ROOT = os.getcwd()
DB = os.path.join(ROOT, "compliance.db")
BACKUPS = os.path.join(ROOT, "backups")
os.makedirs(BACKUPS, exist_ok=True)

def ts():
    return time.strftime("%Y%m%d_%H%M%S")

def integrity(conn):
    try:
        row = conn.execute("PRAGMA integrity_check;").fetchone()
        return row[0] if row else "unknown"
    except Exception as e:
        return f"ERR({e.__class__.__name__})"

def rebuild_fts(conn):
    try:
        conn.execute("INSERT INTO document_chunks_fts(document_chunks_fts) VALUES('rebuild');")
        conn.commit()
        print("[fts] document_chunks_fts rebuilt.")
    except Exception as e:
        print(f"[fts] skip (no table or rebuild failed): {e}")

def quick_backup():
    dst = os.path.join(BACKUPS, f"compliance_pre_repair_{ts()}.db")
    shutil.copy2(DB, dst)
    print(f"[backup] copied to {dst}")
    return dst

def try_backup_api():
    print("[repair] Attempting SQLite backup API...")
    src = sqlite3.connect(DB)
    dst_path = os.path.join(ROOT, "compliance_new.db")
    try:
        dst = sqlite3.connect(dst_path)
        src.backup(dst)
        dst.commit(); dst.close(); src.close()
        print("[repair] Backup API copy complete.")
        return dst_path
    except Exception as e:
        try: dst.close()
        except: pass
        try: src.close()
        except: pass
        try:
            if os.path.exists(dst_path): os.remove(dst_path)
        except: pass
        print("[repair] Backup API failed:", e)
        return None

def try_iterdump_salvage():
    print("[repair] Attempting iterdump salvage...")
    dump_path = os.path.join(BACKUPS, f"dump_{ts()}.sql")
    new_db_path = os.path.join(ROOT, "compliance_new.db")

    # generate dump
    try:
        src = sqlite3.connect(DB)
        with open(dump_path, "w", encoding="utf-8") as f:
            for line in src.iterdump():
                f.write(f"{line}\n")
        src.close()
        print(f"[repair] Wrote dump to {dump_path}")
    except Exception as e:
        print("[repair] iterdump failed:", e)
        return None

    # recreate DB from dump
    try:
        if os.path.exists(new_db_path): os.remove(new_db_path)
        dst = sqlite3.connect(new_db_path)
        with open(dump_path, "r", encoding="utf-8") as f:
            sql = f.read()
        dst.executescript(sql)
        dst.commit(); dst.close()
        print("[repair] New DB created from dump.")
        return new_db_path
    except Exception as e:
        print("[repair] restore-from-dump failed:", e)
        try: dst.close()
        except: pass
        try:
            if os.path.exists(new_db_path): os.remove(new_db_path)
        except: pass
        return None

def swap_in_new(new_db_path):
    old_path = os.path.join(BACKUPS, f"compliance_corrupt_{ts()}.db")
    shutil.move(DB, old_path)
    shutil.move(new_db_path, DB)
    print(f"[swap] Old DB moved to {old_path}")
    print(f"[swap] New DB is now {DB}")

def main():
    ap = argparse.ArgumentParser(description="Check & repair compliance.db safely")
    ap.add_argument("--check-only", action="store_true", help="only run integrity check and exit")
    args = ap.parse_args()

    if not os.path.exists(DB):
        print(f"ERROR: {DB} not found.")
        sys.exit(2)

    # backup first
    quick_backup()

    # initial integrity
    con = sqlite3.connect(DB)
    status = integrity(con)
    print(f"[check] integrity_check => {status}")
    con.close()
    if args.check_only:
        sys.exit(0 if status == "ok" else 1)

    if status == "ok":
        # optionally rebuild FTS and vacuum
        con = sqlite3.connect(DB)
        rebuild_fts(con)
        try:
            con.execute("VACUUM;")
            con.execute("REINDEX;")
            con.commit()
            print("[maint] VACUUM + REINDEX complete.")
        except Exception as e:
            print("[maint] VACUUM/REINDEX skipped:", e)
        con.close()
        print("[done] DB was already healthy.")
        sys.exit(0)

    # try backup API repair
    new_path = try_backup_api()
    if not new_path:
        # fallback to iterdump salvage
        new_path = try_iterdump_salvage()
        if not new_path:
            print("[fatal] Could not create a repaired DB.")
            sys.exit(3)

    # sanity-check the new DB
    con2 = sqlite3.connect(new_path)
    status2 = integrity(con2)
    print(f"[check] new DB integrity_check => {status2}")
    if status2 != "ok":
        con2.close()
        print("[fatal] New DB is not clean; aborting swap.")
        sys.exit(4)

    rebuild_fts(con2)
    try:
        con2.execute("VACUUM;"); con2.execute("REINDEX;"); con2.commit()
    except Exception as e:
        print("[maint] VACUUM/REINDEX on new DB skipped:", e)
    con2.close()

    # swap files
    swap_in_new(new_path)

    print("[success] Repair complete.")
    sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(5)
