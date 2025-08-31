#!/usr/bin/env python
import os, re, csv, json, time, argparse, sqlite3, subprocess
from urllib.parse import urlparse

DB = os.path.join(os.getcwd(), "compliance.db")
SEEDS = os.path.join(os.getcwd(), "config", "seeds.json")
REPORTS_DIR = os.path.join(os.getcwd(), "reports")
QA_CSV = os.path.join(REPORTS_DIR, "qa_social_coverage.csv")

# --- STRICT FILTERS (keep it tight & explainable) -----------------------------
# Only keep URLs with these legal-ish path hints
ALLOW_PATH_RE = re.compile(
    r"(?i)"
    r"(privacy|data[-_/ ]?(policy|protection|processing|control|rights|transfer|retention)|"
    r"terms|tos|conditions|legal|policy|policies|"
    r"cookie(s|[-_/ ]?policy|[-_/ ]?notice)|"
    r"safety|security|guardian|parent|children|minors?|"
    r"account[-_/ ]?(delet|remov|eras|closure|terminate)|"
    r"moderation|community[-_/ ]?guidelines|rules|"
    r"advertis|ads?|target|profil(ing|e)|"
    r"gdpr|ccpa|cpra|dsa|dma|eprivacy|"
    r"transparency|consent|rights|reporting|appeals?)"
)

# Subdomains we allow besides bare domain + www
ALLOW_SUBS = {"www","help","support","about","legal","policy","policies","privacy","business","company","docs","safety","security"}

# Hard block obvious junk before hydration
DENY_HINTS_RE = re.compile(
    r"(?i)(/sitemap|sitemaps?/|\.xml(\.gz)?$|/tag/|/category/|/feed/|/news/|/press/|/blog/|/investor)"
)

def run(cmd: list, check=True) -> subprocess.CompletedProcess:
    print("[run]", " ".join(cmd))
    return subprocess.run(cmd, capture_output=False, text=True, check=check)

def load_roots(keys_csv: str) -> list:
    if not os.path.exists(SEEDS):
        raise SystemExit(f"[ERR] seeds file not found: {SEEDS}")
    with open(SEEDS, "r", encoding="utf-8") as f:
        seeds = json.load(f)

    keys = [k.strip() for k in keys_csv.split(",")] if keys_csv else list(seeds.keys())
    roots = []
    for k in keys:
        roots.extend(seeds.get(k, []))
    # de-dupe, keep order
    seen, out = set(), []
    for r in roots:
        if r and r not in seen:
            seen.add(r); out.append(r.rstrip("/"))
    return out

def domain_of(root: str) -> str:
    host = urlparse(root).netloc.lower()
    return host

def allowed_host(host: str, domain: str) -> bool:
    host = (host or "").lower()
    domain = domain.lower()
    if host == domain: return True
    if host == f"www.{domain}": return True
    # allow only specific first labels (e.g., help.instagram.com)
    if host.endswith("." + domain):
        first = host[: -(len(domain) + 1)]
        return first in ALLOW_SUBS
    return False

def strict_prune(db_path: str, roots: list) -> int:
    """Remove non-legal, non-allowed-host, and sitemap/feed URLs from documents & queue."""
    domains = {domain_of(r): r for r in roots}

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("SELECT url FROM documents")
    urls = [r[0] for r in cur.fetchall()]

    to_del_docs, to_del_q = [], []
    for u in urls:
        try:
            p = urlparse(u)
            host = p.netloc
            pathq = (p.path or "") + (("?" + p.query) if p.query else "")

            # figure which root domain this URL belongs to (the longest matching domain)
            matched = None
            for d in domains.keys():
                if host == d or host.endswith("." + d):
                    matched = d; break
            if not matched:
                to_del_docs.append(u); to_del_q.append(u); continue

            # host gate
            if not allowed_host(host, matched):
                to_del_docs.append(u); to_del_q.append(u); continue

            # path allow & deny
            if DENY_HINTS_RE.search(pathq):
                to_del_docs.append(u); to_del_q.append(u); continue
            if not ALLOW_PATH_RE.search(pathq):
                to_del_docs.append(u); to_del_q.append(u); continue

        except Exception:
            to_del_docs.append(u); to_del_q.append(u)

    deleted = 0
    if to_del_docs:
        cur.executemany("DELETE FROM documents WHERE url=?", [(u,) for u in to_del_docs])
        deleted += cur.rowcount
    if to_del_q:
        cur.executemany("DELETE FROM discovery_queue WHERE url=?", [(u,) for u in to_del_q])

    con.commit(); con.close()
    print(f"[prune] removed {deleted} document rows that were not legal-focused.")
    return deleted

def pending_count(db_path: str) -> int:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM documents WHERE COALESCE(body,'')=''")
    n = cur.fetchone()[0]
    con.close()
    return int(n or 0)

def hydrate_until_done(limit_per_pass=1500, pause=0.2, max_passes=8):
    for i in range(1, max_passes + 1):
        n = pending_count(DB)
        if n == 0:
            print("[hydrate] nothing pending.")
            return
        take = min(n, limit_per_pass)
        print(f"[hydrate] pass {i}: pending={n} take={take}")
        run(["python", os.path.join("scripts", "hydrate_smart.py"), "--limit", str(take), "--pause", str(pause)])
        # small breather for disk
        time.sleep(0.2)
    print("[hydrate] reached max passes; continue later if needed.")

def parse_missing_from_qa(csv_path: str) -> dict:
    """Return {domain: set(missing_categories)} where column == 0."""
    miss = {}
    if not os.path.exists(csv_path):
        return miss
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = row.get("domain") or ""
            if not domain: continue
            missing = set()
            for col in ["privacy","terms","cookies","safety","children","deletion","ads_targeting","moderation","retention"]:
                try:
                    if int(row.get(col, "0")) <= 0:
                        missing.add(col)
                except:
                    missing.add(col)
            if missing:
                miss[domain.strip()] = missing
    return miss

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=str, default="social-tier-1,social-tier-2,social-tier-3,social-tier-4,social-photo-misc",
                    help="Comma-separated keys from config/seeds.json")
    ap.add_argument("--max", type=int, default=300, help="max URLs to queue per root in static discovery")
    ap.add_argument("--scrolls", type=int, default=10, help="playwright scroll steps per root in fallback")
    ap.add_argument("--timeout", type=int, default=25000, help="playwright nav timeout (ms)")
    ap.add_argument("--headed", action="store_true", help="run playwright headed for debugging")
    ap.add_argument("--hydrate-limit", type=int, default=1500, help="max docs per hydration pass")
    ap.add_argument("--no-playwright", action="store_true", help="skip the playwright fallback phase")
    args = ap.parse_args()

    os.makedirs(REPORTS_DIR, exist_ok=True)

    roots = load_roots(args.keys)
    print(f"[info] roots: {len(roots)}")

    # 1) STATIC DISCOVERY
    print("\n=== PASS 1: static discovery ===")
    for r in roots:
        run(["python", os.path.join("scripts", "discover.py"), r, "--max", str(args.max)])

    # 2) PRUNE non-legal/junk BEFORE hydrating
    strict_prune(DB, roots)

    # 3) HYDRATE pass (until done, with cap per pass)
    print("\n=== HYDRATE: static results ===")
    hydrate_until_done(limit_per_pass=args.hydrate_limit)

    # 4) QA snapshot
    print("\n=== QA snapshot (after static) ===")
    run(["python", os.path.join("scripts", "qa_report.py")])
    miss = parse_missing_from_qa(QA_CSV)
    total_missing = len(miss)
    print(f"[qa] domains still missing some categories: {total_missing}")

    if total_missing == 0 or args.no_playwright:
        print("\n[done] coverage looks good or playwright disabled.")
        return

    # 5) PLAYWRIGHT FALLBACK only for missing
    print("\n=== PASS 2: playwright fallback for missing domains ===")
    for dom in sorted(miss.keys()):
        root = f"https://{dom}"
        print(f"\n[playwright] trying: {root}  (missing: {', '.join(sorted(miss[dom]))})")
        cmd = [
            "python", os.path.join("scripts", "discover_playwright.py"),
            root, "--max", "200", "--scrolls", str(args.scrolls), "--timeout", str(args.timeout)
        ]
        if args.headed:
            cmd.append("--headed")
        try:
            run(cmd)
        except subprocess.CalledProcessError:
            print(f"[warn] playwright failed for {root}, continuing…")

    # 6) PRUNE again
    strict_prune(DB, roots)

    # 7) HYDRATE again
    print("\n=== HYDRATE: playwright discoveries ===")
    hydrate_until_done(limit_per_pass=args.hydrate_limit)

    # 8) Final QA snapshot
    print("\n=== FINAL QA snapshot ===")
    run(["python", os.path.join("scripts", "qa_report.py")])
    miss2 = parse_missing_from_qa(QA_CSV)
    print(f"[qa] domains still missing after fallback: {len(miss2)}")
    if miss2:
        print("[qa] still missing for:")
        for d, cats in sorted(miss2.items()):
            print("  -", d, "=>", ", ".join(sorted(cats)))
    print("\n[done] pipeline complete.")

if __name__ == "__main__":
    main()
