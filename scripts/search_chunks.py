import os, sqlite3, argparse, re
from urllib.parse import urlparse

DB_DEFAULT = os.path.join(os.getcwd(), "compliance.db")

def tokenize(s: str):
    return re.findall(r"[A-Za-z0-9']+", (s or "").lower())

def within_window(tokens, a, b, w):
    if not tokens: return False
    pos_a = [i for i,t in enumerate(tokens) if t == a]
    pos_b = [i for i,t in enumerate(tokens) if t == b]
    if not pos_a or not pos_b: return False
    j = 0
    for i in pos_a:
        while j < len(pos_b) and pos_b[j] < i - w:
            j += 1
        k = j
        while k < len(pos_b) and pos_b[k] <= i + w:
            return True
    return False

def parse_near_arg(near_arg: str):
    rules = []
    if not near_arg:
        return rules
    for part in near_arg.split(","):
        bits = part.strip().split(":")
        if len(bits) != 3:
            continue
        t1, t2, win = bits
        try:
            rules.append((t1.lower(), t2.lower(), int(win)))
        except ValueError:
            continue
    return rules

def main():
    ap = argparse.ArgumentParser(description="Search chunk FTS with optional proximity guardrails.")
    ap.add_argument("query", help="FTS5 query string (AND/OR, wildcards, quotes, etc.)")
    ap.add_argument("limit", nargs="?", type=int, default=20, help="Max results to print (fetches more if --near used).")
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--near", default="", help='e.g. "children:data:25,consent:parental:15"')
    args = ap.parse_args()

    near_rules = parse_near_arg(args.near)
    fetch_n = args.limit * 3 if near_rules else args.limit

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    rows = cur.execute("""
        SELECT
          (SELECT url FROM document_chunks WHERE id = document_chunks_fts.rowid) AS url,
          snippet(document_chunks_fts, 1, '[', ']', ' … ', 10) AS snip,
          (SELECT chunk_text FROM document_chunks WHERE id = document_chunks_fts.rowid) AS chunk_text
        FROM document_chunks_fts
        WHERE document_chunks_fts MATCH ?
        LIMIT ?
    """, (args.query, fetch_n)).fetchall()
    con.close()

    out = []
    for url, snip, chunk_text in rows:
        text_for_snippet = snip if snip else (chunk_text or "")[:280]
        if not near_rules:
            out.append((url, text_for_snippet))
        else:
            toks = tokenize(chunk_text or "")
            ok = True
            for t1, t2, w in near_rules:
                if not within_window(toks, t1, t2, w):
                    ok = False
                    break
            if ok:
                out.append((url, text_for_snippet))
        if len(out) >= args.limit:
            break

    if not out:
        print("No matches.")
        return

    for url, snip in out:
        host = "(unknown)"
        if url:
            try: host = urlparse(url).netloc or "(unknown)"
            except: pass
        print(f"• {url or '(unknown)'}")
        print("   " + (snip or "").replace("\n", " ").strip() + "\n")

if __name__ == "__main__":
    main()
