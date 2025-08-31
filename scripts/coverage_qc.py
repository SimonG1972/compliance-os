#!/usr/bin/env python
import os, re, csv, argparse, sqlite3
from urllib.parse import urlparse

DB = os.path.join(os.getcwd(), "compliance.db")

CATS = {
    "privacy": r"(privacy|data[- ]?protection|gdpr|ccpa)",
    "terms": r"(terms|conditions|tos|user[- ]?agreement)",
    "community_rules": r"(community|guidelines|standards|rules|content[- ]?policy)",
    "safety": r"(safety|trust|security)",
    "children_parents": r"(child|children|minor|teen|parent|guardian|youth|kids?)",
    "cookies": r"(cookie|cookies|cookie[- ]?policy|cookie[- ]?notice)",
    "deletion_retention": r"(delete|deletion|erase|erasure|retention|data[- ]?retention|account[- ]?deletion)",
    "ads_targeting": r"(ads?|advertis(e|ing)|target(ing)?|personaliz(e|ation)|profil(ing|e))",
    "appeals_moderation": r"(appeal|moderation|report|enforcement|suspend|ban)"
}
CATS = {k: re.compile(v, re.I) for k, v in CATS.items()}

def domain(u: str) -> str:
    try:
        net = urlparse(u).netloc.lower()
        return net[4:] if net.startswith("www.") else net
    except Exception:
        return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=os.path.join(os.getcwd(), "coverage_social.csv"))
    ap.add_argument("--limit-domains", type=int, default=0, help="0 = all")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()
    rows = cur.execute("""
        SELECT url FROM documents
        WHERE COALESCE(status_code,0) IN (200,304)
    """).fetchall()
    con.close()

    by_dom = {}
    for (u,) in rows:
        d = domain(u)
        if not d: continue
        by_dom.setdefault(d, []).append(u)

    out = []
    for d, urls in by_dom.items():
        flags = {k: False for k in CATS}
        for u in urls:
            for k, pat in CATS.items():
                if pat.search(u):
                    flags[k] = True
        row = {"domain": d, **flags, "total_urls": len(urls)}
        out.append(row)

    # sort by missing count
    out.sort(key=lambda r: sum(1 for k in CATS if not r[k]))

    # optional limit
    if args.limit_domains > 0:
        out = out[:args.limit_domains]

    # write CSV
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["domain", *CATS.keys(), "total_urls"])
        w.writeheader()
        w.writerows(out)

    # console summary
    print(f"Wrote {len(out)} rows to {args.csv}")
    print("Top gaps:")
    for r in out[:20]:
        missing = [k for k in CATS if not r[k]]
        if missing:
            print(f" - {r['domain']}: missing {', '.join(missing)}")

if __name__ == "__main__":
    main()
