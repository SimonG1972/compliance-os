#!/usr/bin/env python
import os, json, sqlite3, argparse
from urllib.parse import urlsplit
from collections import Counter

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def norm_host(h: str) -> str:
    h = h.lower()
    return h[4:] if h.startswith("www.") else h

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    ap.add_argument("--seeds-file", default=None, help="Defaults to config/seeds.json (or seeds.json fallback)")
    ap.add_argument("--keys", default=None, help="Comma-separated keys from seeds.json. If omitted, use ALL keys.")
    ap.add_argument("--out", default=os.path.join(os.getcwd(), "reports", "counts_by_seed.csv"))
    args = ap.parse_args()

    # Pick seeds file
    default_cfg = os.path.join(os.getcwd(), "config", "seeds.json")
    fallback_cfg = os.path.join(os.getcwd(), "seeds.json")
    seeds_path = args.seeds_file or (default_cfg if os.path.exists(default_cfg) else fallback_cfg)
    seeds = load_json(seeds_path)

    # Gather roots from selected keys (or all)
    roots = []
    if args.keys:
        for k in [k.strip() for k in args.keys.split(",")]:
            if k in seeds and isinstance(seeds[k], list):
                roots.extend(seeds[k])
            else:
                print(f"[warn] key not found or not a list: {k}")
    else:
        for v in seeds.values():
            if isinstance(v, list):
                roots.extend(v)

    seed_hosts = sorted({norm_host(urlsplit(u).netloc) for u in roots if u})

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    rows = cur.execute("SELECT url FROM documents").fetchall()
    counts = Counter()
    for (u,) in rows:
        h = norm_host(urlsplit(u).netloc)
        counts[h] += 1

    os.makedirs(os.path.join(os.getcwd(), "reports"), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("domain,count\n")
        for h in seed_hosts:
            f.write(f"{h},{counts.get(h, 0)}\n")

    # Pretty print
    width = max((len(h) for h in seed_hosts), default=10)
    print(f"[wrote] {args.out}")
    print("Counts by seed domain (including 0s):")
    for h in seed_hosts:
        print(f"{h.ljust(width)}  {counts.get(h, 0)}")

if __name__ == "__main__":
    main()
