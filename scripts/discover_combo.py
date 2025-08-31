#!/usr/bin/env python
import argparse, os, sys, subprocess, time, sqlite3, re
from urllib.parse import urlparse

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB = os.path.join(ROOT, "compliance.db")
SCRIPTS = os.path.join(ROOT, "scripts")

def run(cmd:list) -> tuple[int,str]:
    p = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    return p.returncode, p.stdout or ""

def doc_count() -> int:
    con = sqlite3.connect(DB)
    cur = con.cursor()
    try:
        n = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    finally:
        con.close()
    return int(n)

def is_url(s:str) -> bool:
    try:
        u = urlparse(s)
        return u.scheme in ("http","https") and bool(u.netloc)
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser(description="Static discover, then JS fallback if needed.")
    ap.add_argument("root", help="Root URL (e.g. https://www.facebook.com)")
    ap.add_argument("--max", type=int, default=300, help="max to queue per strategy")
    ap.add_argument("--scrolls", type=int, default=12, help="playwright scroll steps")
    ap.add_argument("--timeout", type=int, default=30000, help="playwright per-page timeout (ms)")
    ap.add_argument("--headed", action="store_true", help="run playwright headed for debugging")
    ap.add_argument("--force-js", action="store_true", help="run JS discover regardless of static result")
    ap.add_argument("--hubs", action="store_true", help="also hit common legal/help hub paths via JS")
    args = ap.parse_args()

    if not is_url(args.root):
        print(f"[ERR] not a valid URL: {args.root}")
        sys.exit(2)

    before = doc_count()
    print(f"[static] discover.py {args.root} --max {args.max}")
    rc, out = run([sys.executable, os.path.join(SCRIPTS, "discover.py"), args.root, "--max", str(args.max)])
    print(out.strip())
    if rc != 0:
        print(f"[static] discover.py exit={rc} (continuing to JS fallback if requested)")

    after_static = doc_count()
    new_static = after_static - before
    print(f"[static] new URLs: {new_static}")

    need_js = args.force_js or new_static <= 0
    total_new = new_static

    if need_js:
        # 1) run JS on the root itself
        base_cmd = [sys.executable, os.path.join(SCRIPTS, "discover_playwright.py"), args.root, "--max", str(max(60, args.max)), "--scrolls", str(args.scrolls), "--timeout", str(args.timeout)]
        if args.headed: base_cmd.append("--headed")
        print(f"[js] discover_playwright.py {args.root} (scrolls={args.scrolls}, timeout={args.timeout})")
        rc, out = run(base_cmd)
        print(out.strip())
        if rc != 0:
            print(f"[js] discover_playwright.py exit={rc} (continuing)")

        # 2) optionally hit common legal/help hubs
        if args.hubs:
            hubs = ["/privacy","/policy","/policies","/legal","/terms","/help","/safety","/parents","/family","/cookie","/cookies","/community","/community-standards","/guidelines"]
            for p in hubs:
                hub = args.root.rstrip("/") + p
                print(f"[js] hub {hub}")
                cmd = [sys.executable, os.path.join(SCRIPTS, "discover_playwright.py"), hub, "--max", "120", "--scrolls", str(args.scrolls), "--timeout", str(args.timeout)]
                if args.headed: cmd.append("--headed")
                rc, out = run(cmd)
                print(out.strip())
                time.sleep(0.3)

        after_js = doc_count()
        new_js = after_js - after_static
        total_new += new_js
        print(f"[js] new URLs: {new_js}")

    print(f"[combo] total new URLs this run: {total_new}")
    sys.exit(0)

if __name__ == "__main__":
    main()
