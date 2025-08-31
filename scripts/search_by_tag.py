# scripts/search_by_tag.py
import os, sqlite3, argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tag")
    ap.add_argument("limit", nargs="?", type=int, default=25)
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    rows = cur.execute("""
      SELECT c.url,
             c.chunk_index,
             substr(c.chunk_text,1,260)
      FROM chunk_tags t
      JOIN document_chunks c ON c.id = t.chunk_id
      WHERE t.tag = ?
      LIMIT ?
    """, (args.tag, args.limit)).fetchall()

    if not rows:
        print("No matches.")
        return

    for url, idx, prev in rows:
        print(f"• {url}  [chunk {idx}]")
        print("  ", (prev or "").replace("\n"," "), "\n")

if __name__ == "__main__":
    main()
