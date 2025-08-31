#!/usr/bin/env python
import os, re, sqlite3, argparse

DB = os.path.join(os.getcwd(), "compliance.db")

BAD_LIKE = [
    "%/~gitbook/image%",
    "%/static/%",
    "%/assets/%",
    "%/images/%",
    "%/img/%",
    "%/fonts/%",
    "%/oembed/%",
    "%/embed/%",
    "%.png%", "%.jpg%", "%.jpeg%", "%.gif%", "%.webp%", "%.svg%", "%.ico%",
    "%.mp4%", "%.webm%", "%.mov%", "%.mp3%", "%.wav%", "%.ogg%",
]
BAD_QPARAM = [
    "%width=%", "%dpr=%", "%quality=%", "%token=%", "%sign=%", "%sv=%"
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually delete")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    cur = con.cursor()

    def count(table):
        return cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def find_matches(table):
        conds = " OR ".join(["url LIKE ?"] * (len(BAD_LIKE) + len(BAD_QPARAM)))
        params = BAD_LIKE + BAD_QPARAM
        return cur.execute(f"SELECT url FROM {table} WHERE {conds}", params).fetchall()

    for table in ("discovery_queue", "documents"):
        matches = find_matches(table)
        print(f"[{table}] matches: {len(matches)}")
        if not args.apply:
            for (u,) in matches[:20]:
                print("  -", u)
            if len(matches) > 20:
                print("  ...")
            continue
        # delete
        conds = " OR ".join(["url LIKE ?"] * (len(BAD_LIKE) + len(BAD_QPARAM)))
        params = BAD_LIKE + BAD_QPARAM
        cur.execute(f"DELETE FROM {table} WHERE {conds}", params)
        con.commit()
        print(f"[{table}] deleted.")

    con.close()

if __name__ == "__main__":
    main()
