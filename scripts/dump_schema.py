#!/usr/bin/env python
import os, sqlite3, json

DB = os.path.join(os.getcwd(), "compliance.db")
OUT = os.path.join(os.getcwd(), "reports", "db_schema.txt")

os.makedirs(os.path.dirname(OUT), exist_ok=True)
con = sqlite3.connect(DB)
cur = con.cursor()

with open(OUT, "w", encoding="utf-8") as f:
    def writeln(s=""): f.write(str(s) + "\n")

    writeln(f"[info] DB: {DB}")
    try:
        integrity = cur.execute("PRAGMA integrity_check;").fetchone()[0]
    except Exception as e:
        integrity = f"ERR({type(e).__name__}): {e}"
    writeln(f"[integrity] {integrity}")

    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    ).fetchall()]

    for t in tables:
        writeln(f"\n== {t} ==")
        try:
            schema = cur.execute(
                "SELECT sql FROM sqlite_master WHERE name=?", (t,)
            ).fetchone()[0] or ""
        except Exception as e:
            schema = f"(schema fetch error: {e})"
        writeln(schema)

        try:
            cols = cur.execute(f"PRAGMA table_info('{t}')").fetchall()
            writeln("columns: " + json.dumps(cols, ensure_ascii=False))
        except Exception as e:
            writeln(f"columns err: {e}")

        try:
            cnt = cur.execute(f"SELECT COUNT(*) FROM '{t}'").fetchone()[0]
            writeln(f"count: {cnt}")
        except Exception as e:
            writeln(f"count err: {e}")

con.close()
