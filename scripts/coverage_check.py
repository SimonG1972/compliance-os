#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Check per-host coverage of core legal surfaces and optionally insert missing seeds.
"""
import os, sqlite3, argparse, urllib.parse

DB = os.path.join(os.getcwd(), "compliance.db")
SURFACES = {
  "privacy":   ["/privacy", "/privacy/policy", "/legal/privacy-policy"],
  "terms":     ["/terms", "/terms-of-service", "/legal/terms"],
  "cookies":   ["/cookies", "/cookie-policy"],
  "guidelines":["/community-guidelines", "/guidelines", "/rules"],
  "safety":    ["/safety", "/trust-and-safety"],
  "ads":       ["/ads", "/ad-policy", "/policies/ads"],
  "ip":        ["/copyright", "/dmca", "/ip", "/legal/copyright"],
  "transparency": ["/transparency", "/transparency/center", "/report"],
}

def host_of(url):
    try: return urllib.parse.urlparse(url).hostname.lower()
    except: return ""

def root_of(url):
    p = urllib.parse.urlparse(url); return f"{p.scheme}://{p.hostname}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--insert", action="store_true", help="insert missing seeds into discovery_queue")
    ap.add_argument("--limit", type=int, default=100000)
    args = ap.parse_args()

    con = sqlite3.connect(DB); c = con.cursor()
    # pick one doc per host to get a root
    roots = {}
    for (url,) in c.execute("SELECT url FROM documents LIMIT ?", (args.limit,)):
        h = host_of(url); roots.setdefault(h, root_of(url))

    print("host,surface,have,action")
    inserts = []
    for host, root in roots.items():
        for surf, paths in SURFACES.items():
            # do we already have one?
            have = c.execute("""
              SELECT 1 FROM documents
              WHERE url LIKE ? AND (
                url LIKE ? OR url LIKE ? OR url LIKE ? OR url LIKE ?
              ) LIMIT 1
            """, (f"%{host}%",
                  f"%{surf}%", f"%{surf.replace('-', ' ')}%", "%policy%", "%legal%")).fetchone()
            if have:
                print(f"{host},{surf},1,")
                continue
            # propose first canonical
            seed = urllib.parse.urljoin(root + "/", paths[0].lstrip("/"))
            print(f"{host},{surf},0,seed:{seed}")
            if args.insert:
                inserts.append((seed, f"coverage:auto:{surf}", "queued"))

    if args.insert and inserts:
        c.executemany("INSERT OR IGNORE INTO discovery_queue(url, discovered_from, status) VALUES (?,?,?)", inserts)
        con.commit()
        print(f"[coverage] inserted {len(inserts)} seeds into discovery_queue")
    con.close()

if __name__ == "__main__":
    main()
