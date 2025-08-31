# scripts/search_chunks_counts.py
import os, sqlite3, argparse, collections, urllib.parse

def try_fetch_urls(cur, q, cap=10000):
    sql = """
    SELECT c.url
    FROM document_chunks_fts
    JOIN document_chunks c ON c.id = document_chunks_fts.rowid
    WHERE document_chunks_fts MATCH ?
    LIMIT ?
    """
    return [r[0] for r in cur.execute(sql, (q, cap)).fetchall()]

def host_of(u: str) -> str:
    try:
        return urllib.parse.urlparse(u).netloc or "unknown"
    except Exception:
        return "unknown"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query")
    ap.add_argument("top", nargs="?", type=int, default=20)
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    urls = try_fetch_urls(cur, args.query, cap=50000)
    if not urls:
        print("No matches.")
        return
    ctr = collections.Counter(host_of(u) for u in urls)
    for host, cnt in ctr.most_common(args.top):
        print(f"{host}: {cnt}")

if __name__ == "__main__":
    main()
