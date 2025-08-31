#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/tag_docs.py

Enriched, idempotent tagging for documents.
- Preserves existing schema (tags, document_tags).
- Adds namespaced tags derived from URL + clean_text + policy YAML.
- Safe to re-run; supports --where and --overwrite for subsets.

Examples:
  # Tag all ready docs
  python scripts\\tag_docs.py --where "clean_text IS NOT NULL AND length(clean_text) >= 120"

  # Re-tag only Google policies
  python scripts\\tag_docs.py --overwrite --where "url LIKE '%policies.google.com/%'"

  # Dry-run preview first 15
  python scripts\\tag_docs.py --limit 15 --preview
"""
import os, re, sqlite3, argparse
from datetime import datetime
from urllib.parse import urlparse, parse_qs

DB = os.path.join(os.getcwd(), "compliance.db")

# Try to load policy tagging hints; optional
try:
    from src.policy.loader import get_policy_for_url  # type: ignore
except Exception:
    get_policy_for_url = None

# ------------------ helpers: schema ------------------

def ensure_schema(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id   INTEGER PRIMARY KEY,
            tag  TEXT UNIQUE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS document_tags (
            document_id INTEGER,
            tag_id      INTEGER,
            UNIQUE(document_id, tag_id)
        )
    """)

def upsert_tag(cur, tag: str) -> int:
    tag = tag.strip()
    if not tag:
        return 0
    cur.execute("INSERT OR IGNORE INTO tags(tag) VALUES(?)", (tag,))
    row = cur.execute("SELECT id FROM tags WHERE tag=?", (tag,)).fetchone()
    return int(row[0]) if row else 0

def link_tag(cur, doc_id: int, tag_id: int):
    if not tag_id:
        return
    cur.execute("INSERT OR IGNORE INTO document_tags(document_id, tag_id) VALUES(?,?)", (doc_id, tag_id))

# ------------------ helpers: parsing ------------------

PLATFORM_MAP = {
    "www.youtube.com":"youtube", "youtube.com":"youtube", "policies.google.com":"google", "google.com":"google",
    "x.com":"twitter", "twitter.com":"twitter", "help.twitter.com":"twitter", "help.x.com":"twitter",
    "www.facebook.com":"facebook", "facebook.com":"facebook",
    "www.instagram.com":"instagram", "instagram.com":"instagram",
    "www.linkedin.com":"linkedin", "linkedin.com":"linkedin", "legal.linkedin.com":"linkedin", "help.linkedin.com":"linkedin",
    "www.tiktok.com":"tiktok", "tiktok.com":"tiktok",
    "www.snap.com":"snap", "snap.com":"snap", "www.snapchat.com":"snapchat", "snapchat.com":"snapchat", "values.snap.com":"snap",
    "vimeo.com":"vimeo", "www.vimeo.com":"vimeo",
    "discord.com":"discord", "telegram.org":"telegram",
    "www.pinterest.com":"pinterest", "pinterest.com":"pinterest",
    "www.reddit.com":"reddit", "reddit.com":"reddit",
    "medium.com":"medium", "policy.medium.com":"medium",
    "substack.com":"substack", "www.substack.com":"substack",
    "www.twitch.tv":"twitch", "twitch.tv":"twitch",
    "www.quora.com":"quora", "quora.com":"quora",
}

SURFACE_PATTERNS = [
    (r"privacy|privacy-policy|data[- ]?policy|data[- ]?protection",        "privacy"),
    (r"terms|tos|terms-of-service|conditions",                              "terms"),
    (r"cookie|cookies",                                                     "cookies"),
    (r"guidelines|community[- ]?guidelines|rules|code[- ]?of[- ]?conduct",  "guidelines"),
    (r"safety|trust[- ]?and[- ]?safety|moderation|harm|abuse",              "safety"),
    (r"ads|advertising|ad[- ]?policy|ad[- ]?policies|business[-/]",         "ads"),
    (r"ip|copyright|dmca|intellectual[- ]?property|trademark|brand",        "ip"),
    (r"developer|api|platform[- ]?policy|dev[-/]",                          "developer"),
    (r"transparency|report|enforcement[- ]?report|ad[- ]?transparency",     "transparency"),
    (r"enforcement|penalties|violations|strikes|appeal",                    "enforcement"),
]

AUDIENCE_HINTS = [
    (r"ads|advertiser|business|brand|campaign|iab", "advertisers"),
    (r"creator|partner|monetization|publisher",     "creators"),
    (r"developer|api|sdk|app|apps|dev",             "developers"),
    (r"press|media",                                "publishers"),
    (r"user|consumer|member|account",               "users"),
]

FRAMEWORK_PATTERNS = [
    (r"\bGDPR\b",  "gdpr"),
    (r"\bCCPA\b",  "ccpa"),
    (r"\bCOPPA\b", "coppa"),
    (r"\bDSA\b",   "dsa"),
    (r"\bDMA\b",   "dma"),
    (r"\bDMCA\b",  "dmca"),
    (r"\bHIPAA\b", "hipaa"),
    (r"\bPIPEDA\b","pipeda"),
]

DATE_PATTERNS = [
    # Month D, YYYY  (Jan 2, 2024)
    (re.compile(r"(?:effective|last\s+updated|updated|last\s+modified)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", re.I), ["%B %d, %Y", "%b %d, %Y"]),
    # D Month YYYY   (2 January 2024)
    (re.compile(r"(?:effective|last\s+updated|updated|last\s+modified)\s*[:\-]?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})", re.I), ["%d %B %Y", "%d %b %Y"]),
    # YYYY-MM-DD
    (re.compile(r"(?:effective|last\s+updated|updated|last\s+modified)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})", re.I), ["%Y-%m-%d"]),
]

LOCALE_PARAM_KEYS = ("hl", "lang", "locale")
LOCALE_SEGMENT_RE = re.compile(r"(?<=/)([a-z]{2}(?:-[A-Za-z0-9]{2,6})?)(?=/|$)", re.I)

def host_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""

def infer_platform(host: str) -> str:
    return PLATFORM_MAP.get(host, (host.split(".")[-2] if "." in host else host))

def guess_surface(url_lower: str, text: str) -> list:
    tags = []
    hay = url_lower + " " + (text[:4000] if text else "")
    for pat, name in SURFACE_PATTERNS:
        if re.search(pat, hay, re.I):
            tags.append(f"surface:{name}")
    return list(dict.fromkeys(tags))

def guess_audience(url_lower: str, text: str) -> list:
    tags = []
    hay = url_lower + " " + (text[:4000] if text else "")
    for pat, name in AUDIENCE_HINTS:
        if re.search(pat, hay, re.I):
            tags.append(f"audience:{name}")
    return list(dict.fromkeys(tags))

def guess_doc_type(url: str) -> str:
    return "doc:pdf" if url.lower().endswith(".pdf") else "doc:html"

def parse_locale_region(url: str) -> list:
    t = []
    try:
        p = urlparse(url)
        q = parse_qs(p.query)
        for k in LOCALE_PARAM_KEYS:
            if k in q and q[k]:
                v = q[k][0]
                t.append(f"locale:{v}")
        for m in LOCALE_SEGMENT_RE.finditer(p.path):
            seg = m.group(1)
            if len(seg) in (2, 5, 6, 7, 8):  # en | en-US | zh-Hans etc.
                t.append(f"locale:{seg}")
        # region hints
        u = url.lower()
        for key, region in [
            ("eea", "eea"), ("eu", "eu"), ("/uk/", "uk"), ("-gb", "uk"), ("en-gb", "uk"),
            ("/us/", "us"), ("-us", "us"),
            ("/ca/", "ca"), ("-ca", "ca"),
            ("/au/", "au"), ("-au", "au"),
            ("/in/", "in"), ("-in", "in"),
            ("/jp/", "jp"), ("-jp", "jp"),
        ]:
            if key in u:
                t.append(f"region:{region}")
    except Exception:
        pass
    return list(dict.fromkeys(t))

def detect_frameworks(text: str) -> list:
    if not text:
        return []
    hay = text[:8000]  # cap scan
    out = []
    for pat, name in FRAMEWORK_PATTERNS:
        if re.search(pat, hay, re.I):
            out.append(f"framework:{name}")
    return list(dict.fromkeys(out))

def find_dates(text: str) -> list:
    if not text:
        return []
    hay = text[:12000]
    tags = []
    for regex, fmts in DATE_PATTERNS:
        m = regex.search(hay)
        if not m:
            continue
        raw = m.group(1)
        dt = None
        for fmt in fmts:
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except Exception:
                continue
        if not dt:
            try:
                dt = datetime.fromisoformat(raw)
            except Exception:
                pass
        if dt:
            # decide which field we matched (effective vs updated) by the prefix word
            prefix = hay[max(0, m.start()-12):m.start()].lower()
            tagkey = "effective" if "effective" in prefix else ("updated" if "updated" in prefix or "modified" in prefix else "updated")
            tags.append(f"{tagkey}:{dt.date().isoformat()}")
    return list(dict.fromkeys(tags))

def is_auth_wall(text: str) -> bool:
    if not text:
        return False
    hay = text[:4000].lower()
    return ("log in to continue" in hay) or ("you must log in to continue" in hay) or ("sign in to continue" in hay)

def add_policy_yaml_tags(url: str) -> list:
    if not get_policy_for_url:
        return []
    try:
        p = get_policy_for_url(url)
        # If policy has: tagging: { add: ["surface:privacy", "audience:advertisers", ...], ...}
        extra = []
        tg = getattr(p, "tagging", {}) or {}
        add_list = tg.get("add", []) if isinstance(tg, dict) else []
        for t in add_list:
            if isinstance(t, str) and t.strip():
                extra.append(t.strip())
        # Also flatten any other key->list as policy.<key>=<value>
        for k, v in (tg.items() if isinstance(tg, dict) else []):
            if k == "add":
                continue
            if isinstance(v, list):
                for val in v:
                    if isinstance(val, str) and val.strip():
                        extra.append(f"policy.{k}:{val.strip()}")
        return list(dict.fromkeys(extra))
    except Exception:
        return []

# ------------------ main tagging logic ------------------

def tags_for_document(url: str, clean_text: str) -> list:
    host = host_of(url)
    platform = infer_platform(host)
    url_lower = (url or "").lower()
    tags = set()

    tags.add(f"host:{host}")
    tags.add(f"platform:{platform}")
    tags.add(guess_doc_type(url))

    for t in guess_surface(url_lower, clean_text):  tags.add(t)
    for t in guess_audience(url_lower, clean_text): tags.add(t)
    for t in parse_locale_region(url):              tags.add(t)
    for t in detect_frameworks(clean_text):         tags.add(t)
    for t in find_dates(clean_text):                tags.add(t)

    if is_auth_wall(clean_text): tags.add("auth-wall:true")

    # policy-provided tags from YAML (optional)
    for t in add_policy_yaml_tags(url): tags.add(t)

    # minimal hygiene
    out = []
    for t in sorted(tags):
        t = t.strip().lower()
        if t:
            out.append(t)
    return out

# ------------------ CLI ------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100000, help="max docs to process")
    ap.add_argument("--where", default="clean_text IS NOT NULL AND length(clean_text) >= 120", help="SQL WHERE on documents")
    ap.add_argument("--overwrite", action="store_true", help="delete existing tags for matched docs before tagging")
    ap.add_argument("--preview", action="store_true", help="show tags for first few docs and exit")
    ap.add_argument("--show", type=int, default=0, help="1=per-doc log")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()
    ensure_schema(cur)

    rows = cur.execute(f"""
        SELECT id, url, clean_text
          FROM documents
         WHERE {args.where}
         ORDER BY id
         LIMIT ?
    """, (args.limit,)).fetchall()

    if not rows:
        print("[tag] no matching documents.")
        con.close()
        return

    if args.preview:
        print(f"[preview] showing up to 10 docs (of {len(rows)})")
        for i, (doc_id, url, ct) in enumerate(rows[:10], 1):
            ts = tags_for_document(url or "", ct or "")
            print(f"{i:2}. id={doc_id} url={url}\n    tags: {ts}")
        con.close()
        return

    # Optional: clear existing tags for these docs
    if args.overwrite:
        doc_ids = [(r[0],) for r in rows]
        cur.executemany("DELETE FROM document_tags WHERE document_id = ?", doc_ids)
        con.commit()

    processed = 0
    total_links = 0
    for doc_id, url, clean_text in rows:
        tags = tags_for_document(url or "", clean_text or "")
        if not tags:
            continue
        tag_ids = [upsert_tag(cur, t) for t in tags]
        for tid in tag_ids:
            link_tag(cur, doc_id, tid)
        processed += 1
        total_links += len(tag_ids)
        if args.show:
            print(f"[ok] id={doc_id} tags={len(tag_ids)} url={url}")
        if processed % 200 == 0:
            con.commit()

    con.commit()
    con.close()
    print(f"[tag] done. docs={processed}  tag_links={total_links}")

if __name__ == "__main__":
    main()
