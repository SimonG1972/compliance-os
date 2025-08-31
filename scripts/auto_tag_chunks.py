# scripts/auto_tag_chunks.py
import os, sqlite3, argparse, re
from collections import defaultdict

RULES = {
    "children_data": [
        r"\b(child|children|minor|teen)s?\b",
        r"\b(parental|guardian|age[- ]?verification|consent)\b",
    ],
    "account_deletion": [
        r"\b(delete|deletion|erase|erasure|remove)\b",
        r"\b(account|profile)\b",
    ],
    "advertising_targeting": [
        r"\b(advertis\w*|target\w*|profil\w*)\b",
    ],
    "cookie_retention": [
        r"\bcookies?\b",
        r"\b(retention|expire|expiration|max[- ]?age|duration)\b",
    ],
    "data_transfer": [
        r"\b(transfer|SCCs?|standard contractual clauses|EEA|EU|GDPR)\b",
    ],
    "data_subject_rights": [
        r"\b(access|rectif\w*|correct\w*|portab\w*|erasure|object\w*)\b",
        r"\bright(s)?\b",
    ],
}

COMPILED = {tag: [re.compile(pat, re.I) for pat in pats] for tag, pats in RULES.items()}

def score_tags(text: str) -> dict:
    if not text:
        return {}
    scores = defaultdict(float)
    for tag, patterns in COMPILED.items():
        hits = 0
        for pat in patterns:
            m = pat.findall(text)
            if m:
                hits += len(m)
        if hits:
            scores[tag] = float(hits)
    return scores

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(os.getcwd(), "compliance.db"))
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--min-score", type=float, default=1.0)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    rows = cur.execute("""
      SELECT id, chunk_text FROM document_chunks
      WHERE NOT EXISTS (
        SELECT 1 FROM chunk_tags t WHERE t.chunk_id = document_chunks.id
      )
      LIMIT ?
    """, (args.limit,)).fetchall()

    if not rows:
      print("Nothing new to tag.")
      return

    ins = 0
    for cid, text in rows:
        scores = score_tags(text)
        for tag, score in scores.items():
            if score >= args.min_score:
                try:
                    cur.execute("INSERT OR IGNORE INTO chunk_tags (chunk_id, tag, score) VALUES (?,?,?)",
                                (cid, tag, score))
                    ins += 1
                except sqlite3.IntegrityError:
                    pass
        if ins and ins % 500 == 0:
            con.commit()
            print(f"Tagged so far: {ins}")

    con.commit()
    print(f"Tagged rows inserted: {ins}")

if __name__ == "__main__":
    main()
