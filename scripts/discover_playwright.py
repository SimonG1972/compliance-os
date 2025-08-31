#!/usr/bin/env python
import os, re, sys, json, time, argparse, sqlite3
from urllib.parse import urlparse, urljoin
from typing import List, Set, Tuple

DB = os.path.join(os.getcwd(), "compliance.db")
SEEDS = os.path.join(os.getcwd(), "config", "seeds.json")

LEGAL_HINTS = re.compile(
    r"(privacy|cookies?|cookie-policy|legal|terms|policy|policies|children|safety|data|processing|"
    r"retention|deletion|account|appeal|moderation|ads|advertis|targeting|parent|guardian|consent)",
    re.I,
)

def load_seeds() -> dict:
    if os.path.exists(SEEDS):
        try:
            with open(SEEDS, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}

def same_site(u: str, root_host: str) -> bool:
    try:
        h = urlparse(u).hostname or ""
        return h == root_host or h.endswith("." + root_host)
    except Exception:
        return False

def normalize_abs(href: str, base: str) -> str | None:
    if not href: return None
    href = href.strip()
    if href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("#"):
        return None
    try:
        if not href.startswith("http"):
            href = urljoin(base, href)
        return href.split("#", 1)[0]
    except Exception:
        return None

def collect_links(page, base_url: str, root_host: str, max_links: int) -> List[str]:
    # pull all anchors and some buttons-with-links
    anchors = page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
    # minor scroll/stabilize already handled outside
    urls: List[str] = []
    seen: Set[str] = set()
    for href in anchors or []:
        u = normalize_abs(href, base_url)
        if not u: continue
        if u in seen: continue
        if not same_site(u, root_host): continue
        if not LEGAL_HINTS.search(u):  # URL-level filter (we'll still HTML-fetch later)
            continue
        seen.add(u); urls.append(u)
        if len(urls) >= max_links: break
    return urls

def discover_one(root: str, max_out: int = 200, scrolls: int = 5, timeout_ms: int = 25000, headed: bool = False) -> Tuple[int, List[str]]:
    from playwright.sync_api import sync_playwright
    parsed = urlparse(root)
    if not parsed.scheme:
        root = "https://" + root
        parsed = urlparse(root)
    root_host = parsed.hostname or ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        ctx = browser.new_context()
        page = ctx.new_page()
        try:
            page.goto(root, wait_until="domcontentloaded", timeout=timeout_ms)
        except Exception:
            # try again a bit slower
            try:
                page.goto(root, wait_until="load", timeout=timeout_ms + 10000)
            except Exception:
                pass

        # gentle scrolls to let SPA nav/footers render links
        for _ in range(max(0, scrolls)):
            page.evaluate("window.scrollBy(0, Math.ceil(window.innerHeight*0.9));")
            page.wait_for_timeout(600)

        urls = collect_links(page, root, root_host, max_out)
        browser.close()
    return len(urls), urls

def queue_into_db(urls: List[str], source: str) -> int:
    con = sqlite3.connect(DB)
    cur = con.cursor()
    seen = 0
    for u in urls:
        try:
            cur.execute("INSERT OR IGNORE INTO discovery_queue(url, discovered_from) VALUES (?,?)", (u, source))
            seen += 1
        except Exception:
            pass
    # move into documents now (mirror discover.py behavior)
    cur.execute("SELECT url FROM discovery_queue WHERE status='pending' LIMIT ?", (len(urls),))
    q = [r[0] for r in cur.fetchall()]
    inserted = 0
    for u in q:
        cur.execute("""
            INSERT OR IGNORE INTO documents(url, url_original, title, body, clean_text, status_code, render_mode)
            VALUES (?,?,?,?,?,?,?)
        """, (u, u, "", "", "", None, "static"))
        cur.execute("UPDATE discovery_queue SET status='queued' WHERE url=?", (u,))
        inserted += 1
    con.commit(); con.close()
    return inserted

def run_for_key(key: str, max_per_site: int, scrolls: int, timeout_ms: int, headed: bool) -> None:
    seeds = load_seeds()
    if key not in seeds:
        print(f"[discover] key not found in seeds.json: {key}")
        return
    roots = seeds[key]
    if not roots:
        print(f"[discover] no roots under key: {key}")
        return
    total_added = 0
    for root in roots:
        try:
            cnt, urls = discover_one(root, max_per_site, scrolls, timeout_ms, headed)
            added = queue_into_db(urls, source=f"playwright:{root}")
            print(f"[discover] {root} -> matched:{cnt} | queued:{added}")
            total_added += added
        except Exception as e:
            print(f"[discover] {root} ERROR: {e}")
    print(f"[done] total added: {total_added}")

def main():
    ap = argparse.ArgumentParser(description="JS discovery via Playwright (SPA-friendly).")
    ap.add_argument("root", nargs="?", help="Root URL (e.g., https://snap.com). Omit if using --key.")
    ap.add_argument("--key", help="Group key from config/seeds.json (e.g., social-tier-1)")
    ap.add_argument("--max", type=int, default=200, help="Max URLs per site to queue")
    ap.add_argument("--scrolls", type=int, default=6, help="Number of scroll steps to trigger lazy links")
    ap.add_argument("--timeout", type=int, default=25000, help="Page goto timeout (ms)")
    ap.add_argument("--headed", action="store_true", help="Run a visible browser (not headless)")
    args = ap.parse_args()

    if not os.path.exists(DB):
        print(f"[err] DB not found: {DB}")
        sys.exit(2)

    if args.key:
        run_for_key(args.key, args.max, args.scrolls, args.timeout, args.headed)
        return

    if not args.root:
        ap.error("Provide either a root URL or --key KEY")

    cnt, urls = discover_one(args.root, args.max, args.scrolls, args.timeout, args.headed)
    added = queue_into_db(urls, source=f"playwright:{args.root}")
    print(f"Queued {added} URLs into documents.")

if __name__ == "__main__":
    main()
