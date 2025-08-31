#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Discovery (conservative, legal-only)
- Load roots from config/seeds.json via --key / --keys, or a single positional root.
- Static discovery: robots+sitemaps+canonical seeds, then page link-scan.
- Optional Playwright and JS fallbacks (only if initial results <= --fallback-threshold).
- Strict per-host allowlists + denylists to block junk like:
  - instagram login redirects (?next=..., /accounts/login)
  - youtube /ads*
  - vimeo /blog/*
  - pinterest pins/ideas/search
- Cross-domain allowlist for known policy hosts (e.g., policies.google.com for YouTube).
- Assets filtered (PDF allowed), noise directories filtered, HTML-injected URLs sanitized.
"""

import os, re, sys, json, gzip, io, sqlite3, argparse, urllib.parse, html
from urllib.request import urlopen, Request
from xml.etree import ElementTree as ET

DB_PATH = os.path.join(os.getcwd(), "compliance.db")
DEFAULT_SEEDS_FILE = os.path.join(os.getcwd(), "config", "seeds.json")

UA = "ComplianceOS-Discovery/2.2 (+https://example.local/)"
REQ_TIMEOUT = 20

# ---------------------- URL patterns ----------------------

# Keep only pages that *look* like legal/policy/safety docs.
# NOTE: do NOT include bare 'ads' here (too broad). Only 'ad-policy' variants.
LEGAL_HINTS = re.compile(
    r"(privacy|cookie(?:s|-policy)?|polic(?:y|ies)|legal|terms(?:-of-service)?|conditions|"
    r"community[- ]?guidelines?|safety|moderation|consent|retention|deletion|erasure|account|"
    r"gdpr|ccpa|ad[- ]?polic(?:y|ies))",
    re.I,
)

# Hard blockers for assets (PDF is allowed).
BLOCK_EXT = re.compile(
    r"\.(?:png|jpe?g|gif|webp|svg|ico|css|js|mjs|map|woff2?|ttf|otf|eot|mp4|mov|avi|mkv|mp3|wav|flac)$",
    re.I,
)
ALLOW_EXT = re.compile(r"\.pdf$", re.I)

# Generic noise paths (including gitbook helper dirs)
NOISE_PATH_RE = re.compile(r"/(_next/|static/|assets?/|images?/|fonts?/|~gitbook/)", re.I)

# Canonical seeds we probe per root
CANONICAL_SEEDS = [
    "/privacy", "/privacy/policy",
    "/policy", "/policies",
    "/legal", "/legal/privacy-policy",
    "/terms", "/terms-of-service",
    "/cookies", "/cookie-policy",
    "/help/terms", "/help/privacy",
]

# Cross-domain allowlist for policy hosts
CROSS_ALLOW = {
    # YouTube policies hosted under Google
    "youtube.com": ["policies.google.com"],
    "www.youtube.com": ["policies.google.com"],
    # Snapchat policy hub
    "www.snapchat.com": ["snap.com", "www.snap.com", "values.snap.com"],
    "snapchat.com": ["snap.com", "www.snap.com", "values.snap.com"],
    # Reddit company policies
    "www.reddit.com": ["www.redditinc.com", "redditinc.com"],
    "reddit.com": ["www.redditinc.com", "redditinc.com"],
    # Twitter/X policy surfaces
    "x.com": ["help.twitter.com", "legal.twitter.com", "privacy.x.com", "business.x.com", "developer.x.com", "twitter.com", "www.twitter.com"],
    "www.x.com": ["help.twitter.com", "legal.twitter.com", "privacy.x.com", "business.x.com", "developer.x.com", "twitter.com", "www.twitter.com"],
}

# Host-specific allowlists/denylists (match on endwith host)
HOST_ALLOW_PREFIX = {
    # Instagram: only legal/privacy/terms – block login funnels elsewhere
    "instagram.com": [r"^/legal", r"^/privacy", r"^/terms"],
    "www.instagram.com": [r"^/legal", r"^/privacy", r"^/terms"],

    # Facebook
    "facebook.com": [r"^/privacy", r"^/polic", r"^/policy", r"^/legal", r"^/terms"],
    "www.facebook.com": [r"^/privacy", r"^/polic", r"^/policy", r"^/legal", r"^/terms"],

    # YouTube: allow terms/policies/safety; block /ads entirely
    "youtube.com": [r"^/t/terms", r"^/about/policies", r"^/howyoutubeworks", r"^/safety"],
    "www.youtube.com": [r"^/t/terms", r"^/about/policies", r"^/howyoutubeworks", r"^/safety"],

    # Vimeo: only legal docs
    "vimeo.com": [r"^/terms", r"^/privacy", r"^/cookie", r"^/legal", r"^/policy"],
    "www.vimeo.com": [r"^/terms", r"^/privacy", r"^/cookie", r"^/legal", r"^/policy"],

    # Pinterest
    "pinterest.com": [r"^/policy", r"^/policies", r"^/terms", r"^/privacy"],
    "www.pinterest.com": [r"^/policy", r"^/policies", r"^/terms", r"^/privacy"],

    # LinkedIn: legal lives under /legal or legal.linkedin.com
    "www.linkedin.com": [r"^/legal"],
    "linkedin.com": [r"^/legal"],

    # Discord (primary site)
    "discord.com": [r"^/terms", r"^/privacy", r"^/guidelines"],

    # Twitch
    "www.twitch.tv": [r"^/p/en/legal", r"^/terms", r"^/privacy", r"^/cookie"],
    "twitch.tv": [r"^/p/en/legal", r"^/terms", r"^/privacy", r"^/cookie"],

    # Snapchat/Snap
    "www.snapchat.com": [r"^/terms", r"^/cookie", r"^/privacy", r"^/polic", r"^/policy"],
    "snap.com": [r"^/terms", r"^/polic", r"^/policy", r"^/cookie"],
    "www.snap.com": [r"^/terms", r"^/polic", r"^/policy", r"^/cookie"],
    "values.snap.com": [r"^/privacy", r"^/policy", r"^/safety", r"^/terms", r"^/cookie"],

    # WhatsApp / Messenger
    "www.whatsapp.com": [r"^/legal", r"^/terms", r"^/privacy", r"^/policy", r"^/cookies"],
    "www.messenger.com": [r"^/privacy", r"^/legal", r"^/terms", r"^/policy"],

    # Telegram
    "telegram.org": [r"^/privacy", r"^/terms", r"^/policy", r"^/legal"],

    # Reddit
    "www.reddit.com": [r"^/polic", r"^/policy", r"^/help", r"^/community"],
    # X/Twitter
    "x.com": [r"^/tos", r"^/privacy", r"^/rules", r"^/en/tos"],

    # Medium (policy site is subdomain; allow /legal/ etc)
    "medium.com": [r"^/legal", r"^/privacy", r"^/terms", r"^/cookie"],
    "policy.medium.com": [r"^/"],

    # Substack
    "substack.com": [r"^/privacy", r"^/terms", r"^/cookie", r"^/ccpa"],

    # Quora
    "www.quora.com": [r"^/polic", r"^/policy", r"^/legal", r"^/terms", r"^/cookie"],
    "quora.com": [r"^/polic", r"^/policy", r"^/legal", r"^/terms", r"^/cookie"],

    # TikTok
    "www.tiktok.com": [r"^/legal", r"^/privacy", r"^/policy", r"^/safety", r"^/community-guidelines"],
    "tiktok.com": [r"^/legal", r"^/privacy", r"^/policy", r"^/safety", r"^/community-guidelines"],

    # Threads
    "www.threads.net": [r"^/legal", r"^/privacy", r"^/terms"],
}

HOST_DENY_SUBSTR = {
    # Generic login/redirect junk
    "*": ["?next=", "/accounts/login", "/login.php?next=", "/recover/initiate", "/signin", "/sign-in", "/m/signin"],
    # Pinterest noise
    "pinterest.com": ["/pin/", "/ideas/", "/search/"],
    "www.pinterest.com": ["/pin/", "/ideas/", "/search/"],
    # YouTube ads portal
    "youtube.com": ["/ads"],
    "www.youtube.com": ["/ads"],
    # Vimeo blog content
    "vimeo.com": ["/blog/"],
    "www.vimeo.com": ["/blog/"],
    # Quora noise
    "www.quora.com": ["/q/", "/profile/"],
    "quora.com": ["/q/", "/profile/"],
    # Medium feeds/signin
    "medium.com": ["/feed/", "/m/signin", "/signin"],
    # Instagram: block shop, explore, etc. (in case they pass LEGAL_HINTS)
    "www.instagram.com": ["/explore/", "/shop/"],
    # Reddit: avoid content subs (still allow /policies etc.)
    "www.reddit.com": ["/r/"],
    # Substack topics/blog posts
    "substack.com": ["/topics/", "/p/"],
}

# ---------------------- helpers ----------------------

def sanitize_url(u: str) -> str:
    """Trim HTML tail, stray quotes/backslashes, etc."""
    if not u:
        return ""
    u = html.unescape(u).strip()
    if "<" in u:  # drop accidental HTML fragments
        u = u.split("<", 1)[0]
    u = re.sub(r"[\s'\"\)\]]+$", "", u)  # trailing quotes/brackets
    u = re.sub(r"[\\]+$", "", u)         # trailing backslashes
    u = re.sub(r"[;,]+$", "", u)         # trailing punctuation
    return u

def domain_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""

def same_or_subdomain(target: str, root: str) -> bool:
    return bool(target and root and (target == root or target.endswith("." + root)))

def cross_allowed(target: str, root: str) -> bool:
    allow = CROSS_ALLOW.get(root, [])
    return any(target == a or target.endswith("." + a) for a in allow)

def host_matches(h: str, key: str) -> bool:
    return h == key or h.endswith("." + key)

def prefix_allowed_for_host(host: str, path: str) -> bool:
    for key, pats in HOST_ALLOW_PREFIX.items():
        if host_matches(host, key):
            for p in pats:
                if re.search(p, path, re.I):
                    return True
            return False  # if host listed, must match one of its allows
    return True  # host not listed => pass through (subject to LEGAL_HINTS)

def denied_for_host(host: str, path: str, url: str) -> bool:
    # global denies
    for s in HOST_DENY_SUBSTR.get("*", []):
        if s in url:
            return True
    # host-specific denies
    for key, items in HOST_DENY_SUBSTR.items():
        if key == "*":
            continue
        if host_matches(host, key):
            for s in items:
                if s in url:
                    return True
    return False

def keep_url(u: str, root_host: str) -> bool:
    """Final sieve: same-domain (or cross-allowed), legal-looking, host-allowed, not denied."""
    if not u or not u.lower().startswith(("http://", "https://")):
        return False

    u = sanitize_url(u)

    # urlsplit can throw on bracketed IPv6; treat as reject
    try:
        pu = urllib.parse.urlsplit(u)
    except Exception:
        return False

    # Strip fragment; keep query (locale/lang often matters for legal)
    clean = urllib.parse.urlunsplit((pu.scheme, pu.netloc, pu.path, pu.query, ""))

    # Assets (except PDFs)
    if BLOCK_EXT.search(clean) and not ALLOW_EXT.search(clean):
        return False

    # Generic noise directories
    if NOISE_PATH_RE.search(pu.path):
        return False

    # Host scoping
    thost = pu.netloc.lower()
    if not (same_or_subdomain(thost, root_host) or cross_allowed(thost, root_host)):
        return False

    # Host-specific deny checks (fast-fail)
    if denied_for_host(thost, pu.path, clean):
        return False

    # Host-specific allowlist: if host is listed, path must match one of the prefixes
    if not prefix_allowed_for_host(thost, pu.path):
        return False

    # Must contain legal hints (keeps random support/docs out)
    if not LEGAL_HINTS.search(clean):
        return False

    return True

def uniq_preserve(seq):
    seen = set()
    out = []
    for x in seq:
        if x in seen: 
            continue
        seen.add(x)
        out.append(x)
    return out

def fetch_bytes(url: str):
    try:
        req = Request(url, headers={"User-Agent": UA})
        with urlopen(req, timeout=REQ_TIMEOUT) as r:
            ct = r.headers.get("Content-Type", "") or ""
            data = r.read()
            return data, ct
    except Exception:
        return b"", ""

def fetch_text(url: str):
    data, ct = fetch_bytes(url)
    if not data:
        return "", ""
    # try gzip
    if url.lower().endswith(".gz") or ("gzip" in ct.lower() and not data.startswith(b"<")):
        try:
            data = gzip.decompress(data)
        except Exception:
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as g:
                    data = g.read()
            except Exception:
                pass
    try:
        return data.decode("utf-8", "ignore"), ct
    except Exception:
        return "", ct

ABS_LINK_RE = re.compile(r"https?://[^\s\"'>)\\<]+")

def scan_absolute_links(html_text: str):
    out = []
    for m in ABS_LINK_RE.finditer(html_text or ""):
        out.append(sanitize_url(m.group(0)))
    return out

def insert_urls(con, urls, mode: str, discovered_from: str) -> int:
    cur = con.cursor()
    ins = 0
    for u in urls:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO documents "
                "(url, url_original, title, body, clean_text, status_code, render_mode) "
                "VALUES (?,?,?,?,?,?,?)",
                (u, u, "", "", "", None, mode),
            )
            if cur.rowcount:
                ins += 1
            cur.execute(
                "INSERT OR IGNORE INTO discovery_queue (url, discovered_from, status) "
                "VALUES (?,?,?)",
                (u, discovered_from, "queued"),
            )
        except sqlite3.Error:
            pass
    con.commit()
    return ins

# ---------------------- Static discovery ----------------------

def discover_static_for_root(root: str, hard_cap: int):
    root = root.rstrip("/")
    host = domain_of(root)
    seeds = []

    # robots -> sitemaps
    robots = urllib.parse.urljoin(root + "/", "robots.txt")
    txt, _ = fetch_text(robots)
    candidates = []
    if txt:
        for line in txt.splitlines():
            if "sitemap:" in line.lower():
                sm = line.split(":", 1)[1].strip()
                if sm:
                    candidates.append(sanitize_url(sm))
    for sfx in ("sitemap.xml", "sitemap_index.xml"):
        candidates.append(urllib.parse.urljoin(root + "/", sfx))

    # parse sitemaps
    to_follow = []
    for sm in uniq_preserve(candidates):
        xml, _ = fetch_text(sm)
        if not xml:
            continue
        urls, more = parse_sitemap_urls(xml, hard_cap)
        to_follow.extend(more)
        seeds.extend(urls)

    for sm in uniq_preserve(to_follow):
        if len(seeds) >= hard_cap:
            break
        xml, _ = fetch_text(sm)
        if not xml:
            continue
        urls, _ = parse_sitemap_urls(xml, hard_cap - len(seeds))
        seeds.extend(urls)

    # canonical guesses
    for p in CANONICAL_SEEDS:
        seeds.append(urllib.parse.urljoin(root + "/", p.lstrip("/")))

    # filter
    seeds = [u for u in uniq_preserve(seeds) if keep_url(u, host)]

    # page crawl (seed pages only)
    out, cap_hit = [], False
    for u in seeds:
        out.append(u)
        if len(out) >= hard_cap:
            cap_hit = True
            break
        html_text, _ = fetch_text(u)
        if not html_text:
            continue
        for lu in scan_absolute_links(html_text):
            if keep_url(lu, host):
                out.append(lu)
                if len(out) >= hard_cap:
                    cap_hit = True
                    break
        if cap_hit:
            break

    out = uniq_preserve(out)
    if len(out) > hard_cap:
        out, cap_hit = out[:hard_cap], True
    return out, cap_hit

def parse_sitemap_urls(xml_text: str, max_urls: int):
    urls, smaps = [], []
    try:
        root = ET.fromstring(xml_text)
        ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
        for e in root.findall(f".//{ns}url/{ns}loc"):
            if e.text:
                urls.append(sanitize_url(e.text.strip()))
                if len(urls) >= max_urls:
                    break
        for e in root.findall(f".//{ns}sitemap/{ns}loc"):
            if e.text:
                smaps.append(sanitize_url(e.text.strip()))
    except Exception:
        pass
    return urls, smaps

# ---------------------- Playwright & JS fallbacks ----------------------

def discover_playwright_for_root(root: str, hard_cap: int, scrolls: int, timeout_ms: int, headed: bool):
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return [], False

    root = root.rstrip("/")
    host = domain_of(root)
    collected, cap_hit = [], False

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=(not headed))
            context = browser.new_context(user_agent=UA)
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            try:
                page.goto(root, wait_until="domcontentloaded")
            except Exception:
                pass

            for _ in range(max(1, scrolls)):
                try:
                    hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
                except Exception:
                    hrefs = []
                for h in hrefs:
                    h = sanitize_url(h)
                    if keep_url(h, host):
                        collected.append(h)
                        if len(collected) >= hard_cap:
                            cap_hit = True
                            break
                if cap_hit:
                    break
                try:
                    page.evaluate("window.scrollBy(0, Math.ceil(window.innerHeight*0.9));")
                    page.wait_for_timeout(300)
                except Exception:
                    pass

            context.close()
            browser.close()
    except Exception:
        return [], False

    out = uniq_preserve(collected)
    if len(out) > hard_cap:
        out, cap_hit = out[:hard_cap], True
    return out, cap_hit

def discover_js_fallback(root: str, hard_cap: int):
    root = root.rstrip("/")
    host = domain_of(root)
    seeds = [root] + [urllib.parse.urljoin(root + "/", p.lstrip("/")) for p in CANONICAL_SEEDS]

    out, cap_hit = [], False
    for u in uniq_preserve(seeds):
        if len(out) >= hard_cap:
            cap_hit = True
            break
        html_text, _ = fetch_text(u)
        if not html_text:
            continue
        for lu in scan_absolute_links(html_text):
            if keep_url(lu, host):
                out.append(lu)
                if len(out) >= hard_cap:
                    cap_hit = True
                    break
    out = uniq_preserve(out)
    if len(out) > hard_cap:
        out, cap_hit = out[:hard_cap], True
    return out, cap_hit

# ---------------------- Runner ----------------------

def run_for_root(con, root: str, args):
    print(f"\n=== Discovering {root} ===")
    host = domain_of(root)

    static_urls, static_cap = discover_static_for_root(root, args.max)
    ins_static = insert_urls(con, static_urls, mode="static", discovered_from=f"{root} [static]")
    print(f"[static] found {len(static_urls)} (inserted {ins_static})  cap_hit={bool(static_cap)}")

    need_fallback = (len(static_urls) <= args.fallback_threshold)
    dyn_cap = False

    if need_fallback:
        dyn_urls, dyn_cap = discover_playwright_for_root(root, args.max, args.scrolls, args.timeout, args.headed)
        ins_dyn = insert_urls(con, dyn_urls, mode="dynamic", discovered_from=f"{root} [playwright]")
        print(f"[playwright] found {len(dyn_urls)} (inserted {ins_dyn})  cap_hit={bool(dyn_cap)}")

        if len(dyn_urls) <= args.fallback_threshold:
            js_urls, js_cap = discover_js_fallback(root, args.max)
            ins_js = insert_urls(con, js_urls, mode="js", discovered_from=f"{root} [js-fallback]")
            dyn_cap = dyn_cap or js_cap
            print(f"[js-fallback] found {len(js_urls)} (inserted {ins_js})  cap_hit={bool(js_cap)}")

    if (static_cap or dyn_cap) and args.dyn_max and args.dyn_max > args.max:
        print(f"[heavy] cap hit; re-running {root} with dyn-max={args.dyn_max}")
        static2, _ = discover_static_for_root(root, args.dyn_max)
        ins_s2 = insert_urls(con, static2, mode="static", discovered_from=f"{root} [static:dyn-max]")
        print(f"[static+dynmax] found {len(static2)} (inserted {ins_s2})")

        if len(static2) <= args.fallback_threshold:
            pw2, _ = discover_playwright_for_root(root, args.dyn_max, args.scrolls, args.timeout, args.headed)
            ins_pw2 = insert_urls(con, pw2, mode="dynamic", discovered_from=f"{root} [playwright:dyn-max]")
            print(f"[playwright+dynmax] found {len(pw2)} (inserted {ins_pw2})")

            if len(pw2) <= args.fallback_threshold:
                js2, _ = discover_js_fallback(root, args.dyn_max)
                ins_js2 = insert_urls(con, js2, mode="js", discovered_from=f"{root} [js-fallback:dyn-max]")
                print(f"[js-fallback+dynmax] found {len(js2)} (inserted {ins_js2})")

# ---------------------- Seeds loader ----------------------

def load_roots_from_seeds(keys, seeds_file):
    path = seeds_file or DEFAULT_SEEDS_FILE
    if not os.path.exists(path):
        print(f"[discover] Seeds file not found: {path}")
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[discover] Failed to read seeds: {e}")
        return []

    roots = []
    for k in keys:
        arr = data.get(k, [])
        if isinstance(arr, list):
            roots.extend(arr)

    cleaned = []
    for r in roots:
        r = sanitize_url((r or "").strip())
        if not r:
            continue
        if not r.lower().startswith(("http://", "https://")):
            r = "https://" + r
        cleaned.append(r.rstrip("/"))
    return uniq_preserve(cleaned)

# ---------------------- CLI ----------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", help="Single root URL (e.g., https://example.com)")
    ap.add_argument("--key", help="Single key in seeds.json (e.g., social-tier-1)")
    ap.add_argument("--keys", help="Comma-separated keys in seeds.json")
    ap.add_argument("--seeds-file", default=DEFAULT_SEEDS_FILE, help=f"Path to seeds.json (default: {DEFAULT_SEEDS_FILE})")

    ap.add_argument("--max", type=int, default=400, help="Per-root initial cap (default: 400)")
    ap.add_argument("--dyn-max", type=int, default=0, help="If cap hit, second pass cap (e.g., 2500)")

    ap.add_argument("--scrolls", type=int, default=10, help="Playwright scroll steps (default: 10)")
    ap.add_argument("--timeout", type=int, default=25000, help="Playwright timeout ms (default: 25000)")
    ap.add_argument("--headed", action="store_true", help="Headed browser for Playwright")

    ap.add_argument("--fallback-threshold", type=int, default=0, help="If results <= this, fall back (default: 0)")
    ap.add_argument("--js-seed", action="store_true", help="(compatibility; canonical seeds always tried)")

    args = ap.parse_args()

    # Resolve roots
    if args.keys:
        keys = [k.strip() for k in args.keys.split(",") if k.strip()]
        roots = load_roots_from_seeds(keys, args.seeds_file)
    elif args.key:
        roots = load_roots_from_seeds([args.key], args.seeds_file)
    elif args.root:
        roots = [sanitize_url(args.root.strip())]
    else:
        print("[discover] No roots provided. Use a root URL, --key or --keys from config/seeds.json.")
        sys.exit(1)

    if not roots:
        print("[discover] No valid roots resolved from arguments.")
        sys.exit(1)

    con = sqlite3.connect(DB_PATH)
    try:
        print(f"[info] total unique roots: {len(roots)}")
        for r in roots:
            run_for_root(con, r, args)
    finally:
        con.close()

    print("\n[all done] discovery pass complete.")
    print("Tip: next run hydration, then text_clean, chunking, and tagging.")

if __name__ == "__main__":
    main()
