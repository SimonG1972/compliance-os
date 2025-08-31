#!/usr/bin/env python
import os, re, json, sqlite3, argparse, urllib.parse, datetime
from typing import List, Dict, Tuple, Optional

DB = os.path.join(os.getcwd(), "compliance.db")

# Reuse tight legal filters so we only export true policy docs
LEGAL_PATH_PATTERNS = [
    r"/privacy", r"/data[-_/]?(policy|protection|processing|usage|collection|security)",
    r"/terms", r"/tos\b", r"/eula\b", r"/agreement", r"/user[-_/]?agreement",
    r"/policy", r"/policies", r"/legal", r"/compliance",
    r"/cookie", r"/cookies", r"/cookie[-_/]?policy", r"/cookie[-_/]?notice", r"/cookie[-_/]?settings",
    r"/children", r"/minor", r"/youth", r"/child[-_/]safety", r"/age[-_/]verification",
    r"/safety", r"/moderation", r"/community[-_/]?guidelines", r"/rules",
    r"/delet(e|ion)[-_/]?(account|profile)", r"/account[-_/]?delet",
    r"/retention", r"/data[-_/]?retention",
    r"/gdpr", r"/ccpa", r"/cpra", r"/lgpd", r"/pipa", r"/sccs?", r"/standard[-_/ ]contractual[-_/ ]clauses",
]
LEGAL_RE = re.compile("|".join(f"(?:{p})" for p in LEGAL_PATH_PATTERNS), re.I)

def is_legal_url(u: str) -> bool:
    try:
        pu = urllib.parse.urlparse(u)
    except Exception:
        return False
    if pu.scheme not in ("http","https"): return False
    return bool(LEGAL_RE.search(pu.path or ""))

def slugify(s: str) -> str:
    s = re.sub(r"[^\w.\-]+", "-", s.strip(), flags=re.UNICODE)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "index"

def split_domain_and_slug(url: str) -> Tuple[str, str]:
    pu = urllib.parse.urlparse(url)
    domain = pu.netloc.lower()
    path = pu.path or "/"
    if path.endswith("/"): path += "index"
    # include file extension if present, else use .html for HTML snapshot names
    base = slugify(path.lstrip("/"))
    return domain, base

def load_rows(con, domains: Optional[List[str]], limit: Optional[int]) -> List[Dict]:
    cur = con.cursor()
    where = ["status_code=200"]
    where.append("(coalesce(body,'')<>'' OR coalesce(clean_text,'')<>'')")
    where.append("url like 'http%'")
    where.append("url not like '%/sitemap%.xml%'")
    where.append("url not like '%robots.txt%'")
    where.append("url not like '%.css%' and url not like '%.js%'")
    where.append("url not like '%.png%' and url not like '%.jpg%' and url not like '%.svg%' and url not like '%.webp%'")
    # legal-only
    where.append("1=1")  # placeholder

    sql = f"SELECT url, fetched_at, content_hash, last_modified, etag, render_mode, body, clean_text FROM documents WHERE {' AND '.join(where)}"
    rows = cur.execute(sql).fetchall()

    out = []
    for (url, fetched_at, chash, last_mod, etag, rmode, body, clean) in rows:
        if not is_legal_url(url):
            continue
        if domains:
            host = urllib.parse.urlparse(url).netloc.lower()
            if host not in domains:
                continue
        out.append({
            "url": url,
            "fetched_at": fetched_at,
            "content_hash": chash,
            "last_modified": last_mod,
            "etag": etag,
            "render_mode": rmode,
            "body": body or "",
            "clean_text": clean or ""
        })
    if limit:
        out = out[:limit]
    return out

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def write_text(path: str, text: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def html_wrapper(url: str, fetched_at: Optional[str], body_html: str) -> str:
    banner = f"""
<div style="font-family:system-ui,Segoe UI,Arial,sans-serif;font-size:12px;background:#f5f5f5;border-bottom:1px solid #ddd;padding:8px 10px;color:#333">
  <strong>Snapshot:</strong> {url}
  {"&nbsp;|&nbsp; fetched: " + fetched_at if fetched_at else ""}
</div>
"""
    return f"<!doctype html><html><head><meta charset='utf-8'><title>{url}</title></head><body>{banner}\n{body_html}\n</body></html>"

def try_pdf(url: str, outfile: str, timeout_ms: int = 25000) -> bool:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return False
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context()
            page = ctx.new_page()
            page.goto(url, wait_until="load", timeout=timeout_ms)
            # header/footer with url + page nums
            header = f"<div style='font-size:8px; width:100%; margin-left:8px'>{url}</div>"
            footer = "<div style='font-size:8px; width:100%; margin-left:8px'><span class='pageNumber'></span>/<span class='totalPages'></span>&nbsp;–&nbsp;<span class='date'></span></div>"
            page.pdf(
                path=outfile,
                format="A4",
                print_background=True,
                display_header_footer=True,
                header_template=header,
                footer_template=footer,
                margin={"top":"45px","bottom":"45px","left":"20px","right":"20px"}
            )
            browser.close()
            return True
    except Exception:
        return False

def merge_pdfs(paths: List[str], outpath: str) -> bool:
    try:
        from pypdf import PdfWriter
    except Exception:
        try:
            from PyPDF2 import PdfMerger as _OldMerger  # legacy
        except Exception:
            return False
        try:
            merger = _OldMerger()
            for p in paths:
                merger.append(p)
            ensure_dir(os.path.dirname(outpath))
            with open(outpath, "wb") as f:
                merger.write(f)
            merger.close()
            return True
        except Exception:
            return False
    try:
        writer = PdfWriter()
        for p in paths:
            from pypdf import PdfReader
            reader = PdfReader(p)
            for pg in reader.pages:
                writer.add_page(pg)
        ensure_dir(os.path.dirname(outpath))
        with open(outpath, "wb") as f:
            writer.write(f)
        return True
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser(description="Export hydrated legal documents to HTML/TXT/PDF snapshots.")
    ap.add_argument("--outdir", default=os.path.join(os.getcwd(), "exports"))
    ap.add_argument("--domains", help="Comma-separated hostnames to include (e.g., 'facebook.com,instagram.com'). Default: all.")
    ap.add_argument("--limit", type=int, default=None, help="Limit total exports for a quick dry run.")
    ap.add_argument("--skip-html", action="store_true")
    ap.add_argument("--skip-text", action="store_true")
    ap.add_argument("--pdf", action="store_true", help="Render page PDFs via Playwright Chromium headless.")
    ap.add_argument("--bundle-per-domain", action="store_true", help="Merge per-domain PDFs into a single pack.")
    args = ap.parse_args()

    domains = None
    if args.domains:
        domains = [d.strip().lower() for d in args.domains.split(",") if d.strip()]

    con = sqlite3.connect(DB)
    rows = load_rows(con, domains, args.limit)
    con.close()

    if not rows:
        print("[export] nothing to export (did you hydrate first?)")
        return

    print(f"[export] preparing {len(rows)} snapshots into: {args.outdir}")
    html_root = os.path.join(args.outdir, "html")
    txt_root  = os.path.join(args.outdir, "text")
    pdf_root  = os.path.join(args.outdir, "pdf")
    meta_root = os.path.join(args.outdir, "meta")

    per_domain_pdfs: Dict[str, List[str]] = {}

    for r in rows:
        url = r["url"]
        fetched_at = r["fetched_at"] or ""
        domain, base = split_domain_and_slug(url)

        # META
        meta = {
            "url": url,
            "domain": domain,
            "fetched_at": fetched_at,
            "content_hash": r["content_hash"],
            "last_modified": r["last_modified"],
            "etag": r["etag"],
            "render_mode": r["render_mode"]
        }
        meta_path = os.path.join(meta_root, domain, base + ".json")
        ensure_dir(os.path.dirname(meta_path))
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        # HTML snapshot (use stored HTML body if present; wrap with banner)
        if not args.skip_html and r["body"]:
            html = html_wrapper(url, fetched_at, r["body"])
            html_path = os.path.join(html_root, domain, base + ".html")
            write_text(html_path, html)

        # TEXT snapshot (clean_text if present; else stripped body)
        if not args.skip_text:
            text = r["clean_text"].strip()
            if not text and r["body"]:
                # crude de-html fallback
                text = re.sub(r"<[^>]+>", "", r["body"], flags=re.S).strip()
            if text:
                txt_path = os.path.join(txt_root, domain, base + ".txt")
                write_text(txt_path, text)

        # PDF snapshot (live render)
        if args.pdf:
            pdf_path = os.path.join(pdf_root, domain, base + ".pdf")
            ensure_dir(os.path.dirname(pdf_path))
            ok = try_pdf(url, pdf_path)
            if ok:
                per_domain_pdfs.setdefault(domain, []).append(pdf_path)
            else:
                # If PDF render fails, at least keep HTML/TXT
                pass

    # Bundle “Print Pack” per domain
    if args.pdf and args.bundle_per_domain:
        for domain, paths in per_domain_pdfs.items():
            if not paths: continue
            # stable order
            paths = sorted(paths)
            outp = os.path.join(args.outdir, "bundles", f"{domain}_legal_pack.pdf")
            merged = merge_pdfs(paths, outp)
            if merged:
                print(f"[bundle] {domain}: {outp}")
            else:
                print(f"[bundle] {domain}: merge failed (install pypdf or PyPDF2)")

    print("[export] done.")
    print("Note: snapshots are for internal compliance/reference; always cite the live source URL and date.")
    
if __name__ == "__main__":
    main()
