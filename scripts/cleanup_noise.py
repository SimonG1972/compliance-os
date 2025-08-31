# scripts/cleanup_noise.py
import os, re, sqlite3, argparse
from urllib.parse import urlparse

DB = os.path.join(os.getcwd(), "compliance.db")

NOISE_PATTERNS = [
    r"\.xml(\.gz)?$",            # *.xml, *.xml.gz
    r"/sitemap",                 # /sitemap...
    r"/login", r"/recover",      # login/recover
    r"[?&]next=",                # redirects
]

SAFE_KEEP = [
    r"privacy", r"terms", r"policy", r"policies", r"legal", r"cookie", r"safety",
    r"children", r"parents", r"consent", r"deletion", r"retention", r"ads", r"target"
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--apply", action="store_true", help="actually delete")
    ap.add_argument("--extra", action="append", default=[], help="extra regex pattern(s) to treat as noise")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    patterns = [re.compile(p, re.I) for p in (NOISE_PATTERNS + args.extra)]
    keepers = [re.compile(k, re.I) for k in SAFE_KEEP]

    docs = cur.execute("SELECT url FROM documents").fetchall()
    kill = []
    for (url,) in docs:
        u = url or ""
        if any(k.search(u) for k in keepers):
            continue
        if any(p.search(u) for p in patterns):
            kill.append(u)

    print(f"[dry-run] would delete {len(kill)} documents matching noise patterns.")
    for sample in kill[:20]:
        print("  -", sample)

    if args.apply and kill:
        print("[apply] deleting…")
        # remove from documents + any queued duplicates
        # (no chunks yet, so safe)
        for batch in [kill[i:i+500] for i in range(0, len(kill), 500)]:
            qmarks = ",".join("?"*len(batch))
            cur.execute(f"DELETE FROM documents WHERE url IN ({qmarks})", batch)
            cur.execute(f"DELETE FROM discovery_queue WHERE url IN ({qmarks})", batch)
        con.commit()
        print("[apply] done.")
    con.close()

if __name__ == "__main__":
    main()
