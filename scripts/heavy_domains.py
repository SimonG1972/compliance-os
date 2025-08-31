#!/usr/bin/env python
import os, sqlite3, argparse, urllib.parse, collections

DB = os.path.join(os.getcwd(), "compliance.db")

def host(u: str) -> str:
    try:
        return urllib.parse.urlparse(u).netloc.lower()
    except Exception:
        return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=int, default=350, help="show domains with >= this many URLs")
    ap.add_argument("--top", type=int, default=50, help="max rows to print")
    ap.add_argument("--show-cmds", action="store_true", help="also print discover.py commands to rerun with higher --max")
    ap.add_argument("--new-max", type=int, default=1200, help="--max to suggest for reruns")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()

    counts = collections.Counter()
    for (url,) in cur.execute("SELECT url FROM documents"):
        counts[host(url)] += 1

    rows = [(h, c) for h, c in counts.items() if c >= args.threshold]
    rows.sort(key=lambda x: -x[1])
    rows = rows[:args.top]

    print(f"[heavy] domains with >= {args.threshold} URLs in documents:")
    for h, c in rows:
        root = f"https://{h}"
        print(f"{h:35s} {c:5d}  root: {root}")
        if args.show_cmds:
            print(f"  python .\\scripts\\discover.py {root} --max {args.new_max}")

    con.close()

if __name__ == "__main__":
    main()
