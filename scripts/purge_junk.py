#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Purge junk rows from the database with precise targeting.

- Works on documents, discovery_queue, or both.
- If discovery_queue lacks a `host` column, --hosts are converted to URL LIKE patterns.
- Dry-run by default; use --apply to delete.

Examples:
  python scripts/purge_junk.py --from queue ^
    --hosts developers.soundcloud.com,guide.line.me,help.soundcloud.com,mastodon.social,substack.com,www.substack.com,www.pixiv.net ^
    --like "https://www.youtube.com/cookies%,https://www.youtube.com/transparency%"

  python scripts/purge_junk.py --apply --from documents --hosts discord.com --zero-body
  python scripts/purge_junk.py --apply     # legacy auto-junk for documents
"""

import argparse
import os
import sqlite3
import shutil
from datetime import datetime
from urllib.parse import urlparse

DEFAULT_DB = os.path.join(os.getcwd(), "compliance.db")

def csv_list(s):
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

def backup_db(db_path):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    bak = f"{db_path}.bak_{ts}"
    shutil.copyfile(db_path, bak)
    print(f"[backup] {bak}")
    return bak

def table_has_column(con, table, col):
    cur = con.cursor()
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)

def build_documents_where(args):
    """
    WHERE for documents. Defaults to safe 'auto' heuristics if no explicit filter:
      - status_code in (404,410) OR empty clean_text
    """
    conds, params = [], []

    if args.hosts:
        qmarks = ",".join(["?"] * len(args.hosts))
        conds.append(f"host IN ({qmarks})")
        params.extend(args.hosts)

    if args.like:
        like_conds = []
        for pat in args.like:
            like_conds.append("url LIKE ?")
            params.append(pat)
        if like_conds:
            conds.append("(" + " OR ".join(like_conds) + ")")

    if args.status:
        qmarks = ",".join(["?"] * len(args.status))
        conds.append(f"COALESCE(status_code,0) IN ({qmarks})")
        params.extend(args.status)

    if args.zero_body:
        conds.append("LENGTH(COALESCE(body,''))=0")

    if args.zero_clean:
        conds.append("LENGTH(COALESCE(clean_text,''))=0")

    if not conds:
        conds.append("(COALESCE(status_code,0) IN (404,410) OR LENGTH(COALESCE(clean_text,''))=0)")

    return " AND ".join(conds), params

def hosts_to_url_like_conds(hosts):
    """
    Turn hosts into (url LIKE 'http://h/%' OR url LIKE 'https://h/%') blocks.
    Returns (sql_fragment, params)
    """
    blocks = []
    params = []
    for h in hosts:
        blocks.append("(url LIKE ? OR url LIKE ?)")
        params.append(f"http://{h}/%")
        params.append(f"https://{h}/%")
    return "(" + " OR ".join(blocks) + ")", params if blocks else ("", [])

def build_queue_where(args, con):
    """
    WHERE for discovery_queue. Safety:
      - Never touch items already promoted to documents.
      - If no hosts/like filters, skip (to prevent accidental mass-deletes).
    If queue has no `host` column, emulate host filtering via URL LIKE patterns.
    """
    conds = ["url NOT IN (SELECT url FROM documents)"]
    params = []

    if not (args.hosts or args.like):
        return None, None

    has_host_col = table_has_column(con, "discovery_queue", "host")

    if args.hosts:
        if has_host_col:
            qmarks = ",".join(["?"] * len(args.hosts))
            conds.append(f"host IN ({qmarks})")
            params.extend(args.hosts)
        else:
            frag, p = hosts_to_url_like_conds(args.hosts)
            conds.append(frag)
            params.extend(p)

    if args.like:
        like_conds = []
        for pat in args.like:
            like_conds.append("url LIKE ?")
            params.append(pat)
        if like_conds:
            conds.append("(" + " OR ".join(like_conds) + ")")

    return " AND ".join(conds), params

def preview(con, table, where, params, limit=10):
    cur = con.cursor()
    has_host = table_has_column(con, table, "host")
    if table == "documents":
        sql = f"""SELECT id, host, url, COALESCE(status_code,0) AS sc,
                  LENGTH(COALESCE(clean_text,'')) AS L
                  FROM documents WHERE {where} LIMIT {limit}"""
        rows = cur.execute(sql, params).fetchall()
        for _id, host, url, sc, L in rows:
            print(f"    - doc_id={_id} host={host} sc={sc} clean_len={L} {url}")
        return rows
    else:
        if has_host:
            sql = f"SELECT id, host, url FROM discovery_queue WHERE {where} LIMIT {limit}"
            rows = cur.execute(sql, params).fetchall()
            for _id, host, url in rows:
                print(f"    - q_id={_id} host={host} {url}")
            return rows
        else:
            sql = f"SELECT id, url FROM discovery_queue WHERE {where} LIMIT {limit}"
            rows = cur.execute(sql, params).fetchall()
            for _id, url in rows:
                host = urlparse(url).netloc
                print(f"    - q_id={_id} host={host or '-'} {url}")
            return rows

def count_matches(con, table, where, params):
    cur = con.cursor()
    sql = f"SELECT COUNT(1) FROM {table} WHERE {where}"
    return cur.execute(sql, params).fetchone()[0]

def delete_rows(con, table, where, params):
    cur = con.cursor()
    sql = f"DELETE FROM {table} WHERE {where}"
    cur.execute(sql, params)
    return cur.rowcount

def main():
    p = argparse.ArgumentParser(description="Purge junk rows from documents and/or discovery_queue.")
    p.add_argument("--db", default=DEFAULT_DB, help="Path to compliance.db")
    p.add_argument("--from", dest="from_table", choices=["documents", "queue", "both"], default="both",
                   help="Which table(s) to purge from (default: both)")
    p.add_argument("--hosts", type=str, default="",
                   help="Comma-separated host list (e.g. 'substack.com,www.substack.com')")
    p.add_argument("--like", type=str, default="",
                   help="Comma-separated URL LIKE patterns (use % wildcards)")
    p.add_argument("--status", type=str, default="",
                   help="Comma-separated integer status codes for documents (e.g. '404,410')")
    p.add_argument("--zero-body", action="store_true", help="(documents) Purge rows where body is empty")
    p.add_argument("--zero-clean", action="store_true", help="(documents) Purge rows where clean_text is empty")
    p.add_argument("--apply", action="store_true", help="Apply changes (otherwise dry-run)")
    args = p.parse_args()

    args.hosts = csv_list(args.hosts)
    args.like = csv_list(args.like)
    args.status = [int(x) for x in csv_list(args.status)]

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    con = sqlite3.connect(args.db)

    tasks = []

    if args.from_table in ("documents", "both"):
        w, p_ = build_documents_where(args)
        tasks.append(("documents", w, p_))

    if args.from_table in ("queue", "both"):
        w, p_ = build_queue_where(args, con)
        if w:
            tasks.append(("discovery_queue", w, p_))
        elif args.from_table == "queue":
            print("[warn] No explicit --hosts/--like filters for queue; skipping for safety.")

    # Preview
    total = 0
    for table, where, params in tasks:
        cnt = count_matches(con, table, where, params)
        print(f"[{table}] candidates: {cnt}")
        if cnt:
            preview(con, table, where, params, limit=10)
        total += cnt

    if not args.apply:
        print("\n[dry-run] No changes applied. Re-run with --apply to delete.")
        con.close()
        return

    if total == 0:
        print("[apply] Nothing to delete.")
        con.close()
        return

    backup_db(args.db)

    deleted_total = 0
    for table, where, params in tasks:
        n = delete_rows(con, table, where, params)
        print(f"[purge] deleted from {table}: {n}")
        deleted_total += n

    con.commit()
    con.close()
    print(f"[done] total deleted: {deleted_total}")

if __name__ == "__main__":
    main()
