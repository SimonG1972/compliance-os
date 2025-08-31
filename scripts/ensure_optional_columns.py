# scripts/ensure_optional_columns.py
import sqlite3

def ensure_column(cur, table, col, coltype):
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")

con = sqlite3.connect("compliance.db")
cur = con.cursor()

# Documents meta
ensure_column(cur, "documents", "policy_host", "TEXT")
ensure_column(cur, "documents", "policy_version", "TEXT")
ensure_column(cur, "documents", "render_used", "INTEGER")  # 0/1

con.commit()
con.close()
print("Optional columns ensured.")
