# scripts/discover_sitemaps.py
import os, re, sys, io, gzip, sqlite3, argparse
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen, Request
from xml.etree import ElementTree as ET

DB = os.path.join(os.getcwd(), "compliance.db")
UA = "ComplianceOS-DiscoverSitemaps/1.0"

LEGAL_HINTS = re.compile(
    r"(privacy|cookie|cookies|legal|terms|policy|policies|children|parents|safety|consent|delet|retention|appeal|moderation|ads|advertis|targeting|profiling|gdpr|ccpa)",
    re.I,
)

def http_get(u, timeout=20):
    try:
        with urlopen(Request(u, headers={"User-Agent": UA}), timeout=timeout) as r:
            raw = r.read()
            ctype = r.headers.get("Content-Type","").lower()
            return raw, ctype, 200
    except Exception as e:
        return b"", "", 0

def parse_xml(raw, ctype):
    # gunzip if needed
    if b"\x1f\x8b" == raw[:2] or "gzip" in ctype:
        try:
            raw = gzip.decompress(raw)
        except Exception:
            try:
                raw = io.BytesIO(raw).read()
                raw = gzip.decompress(raw)
            except Exception:
                pass
    try:
        return ET.fromstring(raw)
    except Exception:
        return None

def iter_sitemaps(root_el):
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    # index -> <sitemap><loc>...
    for loc in root_el.findall(f".//{ns}sitemap/{ns}loc"):
        if loc.text: yield loc.text.strip()

def iter_urls(root_el):
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    for loc in root_el.findall(f".{ns}url/{ns}loc"):
        if loc.text: yield loc.text.strip()
    # more tolerant:
    for loc in root_el.findall(".//loc"):
        if loc.text: yield loc.text.strip()

def discover_from_robots(root, max_urls):
    robots_url = urljoin(root, "/robots.txt")
    raw, ctype, code = http_get(robots_url)
    if code != 200: return []

    smaps = []
    for line in raw.decode("utf-8","ignore").splitlines():
        if line.lower().startswith("sitemap:"):
            sm = line.split(":",1)[1].strip()
            if not sm: continue
            if sm.startswith("http"): smaps.append(sm)
            else: smaps.append(urljoin(root, sm))

    out = []
    for sm in smaps:
        raw, ctype, code = http_get(sm)
        if code != 200: continue
        el = parse_xml(raw, ctype)
        if el is None: continue

        # recurse if index
        more = list(iter_sitemaps(el))
        if more:
            for child in more:
                raw2, ctype2, code2 = http_get(child)
                if code2 != 200: continue
                el2 = parse_xml(raw2, ctype2)
                if el2 is None: continue
                for u in iter_urls(el2):
                    if LEGAL_HINTS.search(u or ""):
                        out.append(u)
                        if len(out) >= max_urls: return out
            continue

        # direct urlset
        for u in iter_urls(el):
            if LEGAL_HINTS.search(u or ""):
                out.append(u)
                if len(out) >= max_urls: return out
    return out

def enqueue(con, urls, src):
    cur = con.cursor()
    seen = set()
    ins = 0
    for u in urls:
        if not u or u in seen: continue
        seen.add(u)
        cur.execute("INSERT OR IGNORE INTO discovery_queue(url, discovered_from) VALUES (?,?)", (u, src))
        cur.execute("""INSERT OR IGNORE INTO documents
                       (url, url_original, title, body, clean_text, status_code, render_mode)
                       VALUES (?,?,?,?,?,?,?)""", (u, u, "", "", "", None, "static"))
        ins += 1
    con.commit()
    return ins

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("roots", nargs="*", help="Root sites like https://www.facebook.com")
    ap.add_argument("--max", type=int, default=500, help="max URLs per root")
    ap.add_argument("--seeds", default=os.path.join(os.getcwd(),"config","seeds.json"),
                    help="optional seeds.json (use a key with --key)")
    ap.add_argument("--key", help="key in seeds.json to run (e.g., social-tier-2)")
    args = ap.parse_args()

    roots = list(args.roots)
    if args.key:
        try:
            import json
            with open(args.seeds, "r", encoding="utf-8") as f:
                data = json.load(f)
            roots += data.get(args.key, [])
        except Exception:
            pass

    if not roots:
        print("No roots provided.")
        return

    con = sqlite3.connect(DB)
    total = 0
    for r in roots:
        r = r.rstrip("/")
        print(f"[discover] robots+sitemaps on {r}")
        urls = discover_from_robots(r, args.max)
        added = enqueue(con, urls, "robots+sitemap")
        total += added
        print(f"  added {added} relevant URL(s)")
    con.close()
    print(f"[done] total added: {total}")

if __name__ == "__main__":
    main()
