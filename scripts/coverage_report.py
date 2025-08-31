# scripts/coverage_report.py
import sqlite3

def main():
    con = sqlite3.connect("compliance.db")
    cur = con.cursor()

    # Does category exist?
    cols = [r[1] for r in cur.execute("PRAGMA table_info(documents)")]
    if "category" in cols:
        group_field = "category"
    else:
        group_field = "'uncategorized'"

    print("\n=== Coverage by Category ===")
    for row in cur.execute(f"""
        SELECT {group_field},
               COUNT(*) as total,
               SUM(CASE WHEN clean_text IS NOT NULL THEN 1 ELSE 0 END) as cleaned,
               SUM(CASE WHEN length(clean_text) >= 120 THEN 1 ELSE 0 END) as useful
        FROM documents
        GROUP BY {group_field}
        ORDER BY total DESC
    """):
        print(row)

    print("\n=== Pending in discovery_queue ===")
    for row in con.execute("""
        SELECT category, COUNT(*) FROM discovery_queue
        WHERE status='pending'
        GROUP BY category
    """):
        print(row)

    con.close()

if __name__ == "__main__":
    main()
