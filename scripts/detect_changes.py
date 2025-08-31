# scripts/detect_changes.py
import os, sys, csv, sqlite3, re
from datetime import datetime
from difflib import ndiff

DB = os.path.join(os.getcwd(), "compliance.db")

def table_exists(cur, name):
    return cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,)
    ).fetchone() is not None

def cols(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def pick(colnames, patterns, prefer=None):
    """
    Pick the first column whose lowercase name matches any regex in patterns.
    If prefer (list) is provided, sort candidates to try those names first.
    """
    lower = [c.lower() for c in colnames]
    cand = []
    for i, c in enumerate(lower):
        for pat in patterns:
            if re.search(pat, c):
                cand.append(colnames[i])
                break
    if prefer:
        prefer_l = [p.lower() for p in prefer]
        cand.sort(key=lambda x: (0 if x.lower() in prefer_l else 1, x))
    return cand[0] if cand else None

def tiny_diff(a, b, max_chars=280):
    if not a or not b:
        return ""
    a = " ".join(a.split())
    b = " ".join(b.split())
    if a == b:
        return ""
    out = []
    for tok in ndiff(a.split(), b.split()):
        if tok.startswith(("+ ", "- ")):
            out.append(tok)
        if len(" ".join(out)) > max_chars:
            break
    return " ".join(out)[:max_chars]

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--csv", default="changes_latest.csv")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    if not table_exists(cur, "document_revisions"):
        print("No document_revisions table found. Run your revisions init first.")
        sys.exit(1)

    c = cols(cur, "document_revisions")
    cl = [x.lower() for x in c]

    # --- Map the essential columns dynamically ---
    # URL
    url_col = pick(c, [r"\burl\b", r"\bpage\b"], prefer=["url"])

    # Hash columns
    prev_hash_col = pick(
        c,
        [r"(prev|old|before|from).*hash", r"\bhash_?(prev|old|from)\b"],
        prefer=["prev_hash", "old_hash", "before_hash", "from_hash", "prev_content_hash", "old_content_hash"]
    )
    new_hash_col = pick(
        c,
        [r"(new|curr|after|to).*hash", r"\bhash_?(new|curr|to)\b"],
        prefer=["new_hash", "curr_hash", "after_hash", "to_hash", "new_content_hash"]
    )

    # If we still couldn't find them, last resort: any two columns containing 'hash'
    if not prev_hash_col or not new_hash_col:
        hash_like = [col for col in c if "hash" in col.lower()]
        if len(hash_like) >= 2:
            # Heuristic: older-looking first
            candidates = sorted(hash_like)
            prev_hash_col = prev_hash_col or candidates[0]
            new_hash_col  = new_hash_col  or candidates[1]

    # Text columns (optional)
    prev_text_col = pick(
        c,
        [r"(prev|old|before).*text", r"(prev|old|before).*(body|clean)", r"\btext_?(prev|old)\b", r"\b(body|clean)_?(prev|old)\b"],
        prefer=["prev_text", "old_text", "prev_body", "old_body", "prev_clean", "old_clean"]
    )
    new_text_col = pick(
        c,
        [r"(new|curr|after).*text", r"(new|curr|after).*(body|clean)", r"\btext_?(new|curr)\b", r"\b(body|clean)_?(new|curr)\b"],
        prefer=["new_text", "curr_text", "new_body", "curr_body", "new_clean", "curr_clean"]
    )

    # Timestamp (for ordering/printing)
    ts_col = pick(
        c,
        [r"changed_at", r"updated_at", r"created_at", r"\bts\b", r"time", r"datetime"],
        prefer=["changed_at", "updated_at", "created_at"]
    )

    # Change kind (optional)
    kind_col = pick(c, [r"change_kind", r"reason", r"type"], prefer=["change_kind", "reason"])

    # Validate the essentials
    missing = []
    if not url_col:        missing.append("url")
    if not prev_hash_col:  missing.append("prev_hash")
    if not new_hash_col:   missing.append("new_hash")

    if missing:
        print("❌ Could not locate required columns in document_revisions.")
        print("   Required logical roles:", missing)
        print("   Available columns:", ", ".join(c))
        print("   Tip: ensure your trigger inserts prev/new hash columns, e.g. prev_hash/new_hash.")
        sys.exit(1)

    order_expr = f"datetime({ts_col})" if ts_col else "rowid"

    select_cols = [url_col, prev_hash_col, new_hash_col]
    if ts_col:   select_cols.append(ts_col)
    if kind_col: select_cols.append(kind_col)
    if prev_text_col: select_cols.append(prev_text_col)
    if new_text_col:  select_cols.append(new_text_col)

    # Build SQL safely
    quoted = ", ".join([f'"{col}"' for col in select_cols])
    sql = f"""
        SELECT {quoted}
        FROM document_revisions
        WHERE COALESCE("{prev_hash_col}",'') != COALESCE("{new_hash_col}",'')
        ORDER BY {order_expr} DESC
        LIMIT ?
    """

    try:
        rows = cur.execute(sql, (args.limit,)).fetchall()
    except sqlite3.OperationalError as e:
        print("❌ SQLite error while selecting changes:", e)
        print("SQL was:\n", sql)
        sys.exit(1)

    if not rows:
        print("No changes found.")
        con.close()
        return

    # Map indexes
    idx = {col: i for i, col in enumerate(select_cols)}

    def get(r, col):
        return r[idx[col]] if col in idx else ""

    print(f"Latest {len(rows)} changes:")
    print("-" * 80)
    out_rows = []
    for r in rows:
        url = get(r, url_col)
        ph  = get(r, prev_hash_col) or ""
        nh  = get(r, new_hash_col) or ""
        ts  = get(r, ts_col) if ts_col else ""
        kind = get(r, kind_col) if kind_col else ""

        short = ""
        if prev_text_col and new_text_col:
            short = tiny_diff(get(r, prev_text_col) or "", get(r, new_text_col) or "")

        print(f"• {url}")
        if ts:
            print(f"  when: {ts}")
        if kind:
            print(f"  kind: {kind}")
        print(f"  prev_hash: {ph[:12]}… → new_hash: {nh[:12]}…")
        if short:
            print(f"  diff: {short}")
        print()

        out_rows.append({
            "url": url,
            "changed_at": ts,
            "change_kind": kind,
            "prev_hash": ph,
            "new_hash": nh,
            "short_diff": short,
        })

    # Write CSV snapshot
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["url","changed_at","change_kind","prev_hash","new_hash","short_diff"])
        w.writeheader()
        w.writerows(out_rows)

    print(f"Wrote {args.csv}")
    con.close()

if __name__ == "__main__":
    main()
