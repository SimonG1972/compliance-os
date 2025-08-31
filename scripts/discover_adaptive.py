#!/usr/bin/env python
import os, sqlite3, subprocess

DB = os.path.join(os.getcwd(), "compliance.db")
DISCOVER = os.path.join("scripts", "discover.py")

# thresholds for successive sweeps
MAX_STEPS = [400, 1200, 2000]

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Find roots currently queued in documents
    cur.execute("SELECT DISTINCT url FROM documents ORDER BY url")
    all_urls = [r[0] for r in cur.fetchall()]
    con.close()

    roots = sorted({u.split('/')[2] for u in all_urls if u.startswith("http")})
    print(f"[info] {len(roots)} roots in DB")

    for root in roots:
        for step, maxval in enumerate(MAX_STEPS, 1):
            print(f"\n=== Sweep {step} for {root} with --max {maxval} ===")
            url = f"https://{root}"
            rc = subprocess.call(["python", DISCOVER, url, "--max", str(maxval)])
            if rc != 0:
                print(f"[warn] discover failed for {url} at max {maxval}")
                break
            # check if we actually hit the ceiling
            con = sqlite3.connect(DB)
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM documents WHERE url LIKE ?", (f"%{root}%",))
            cnt = cur.fetchone()[0]
            con.close()
            print(f"[check] {root}: {cnt} discovered so far")
            if cnt < maxval:  # no longer at ceiling
                print(f"[done] {root} saturated at {cnt}")
                break

if __name__ == "__main__":
    main()
