# scripts/policy_report.py
import sqlite3, json

con = sqlite3.connect("compliance.db")
cur = con.cursor()

def val(sql):
    try: return cur.execute(sql).fetchall()
    except Exception: return []

print("== hydration by policy_host ==")
for host, cnt in val("SELECT COALESCE(policy_host,'(null)'), COUNT(*) FROM documents GROUP BY policy_host ORDER BY 2 DESC"):
    print(f"{host:30} {cnt}")

print("\n== render_used breakdown ==")
for used, cnt in val("SELECT COALESCE(render_used,0), COUNT(*) FROM documents GROUP BY render_used ORDER BY 2 DESC"):
    print(f"render_used={used} -> {cnt}")

print("\n== top 15 domains in discovery_queue by status ==")
rows = val("""SELECT substr(url, instr(url,'://')+3, instr(substr(url, instr(url,'://')+3),'/')-1) AS host,
                     status, COUNT(*)
              FROM discovery_queue
              GROUP BY host, status
              ORDER BY COUNT(*) DESC
              LIMIT 15""")
for host, status, cnt in rows:
    print(f"{host:30} status={status:10} {cnt}")

con.close()
