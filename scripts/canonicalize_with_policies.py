#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/canonicalize_with_policies.py

Canonicalize & Dedupe using policy normalization (no full wipe).
- DRY-RUN by default: shows what WOULD change.
- Use --apply to actually write changes (creates a .bak backup first).
- Merges duplicates produced by locale/tracker params (e.g., YouTube ?hl=, override_hl=).
- Updates discovery_queue accordingly and preserves best content.

Winner selection per canonical group:
  1) Prefer status_code in {200, 304}
  2) Then longer body length
  3) Then newest fetched_at

Usage:
  # Dry run
  python -m scripts.canonicalize_with_policies
  # or
  python scripts\\canonicalize_with_policies.py

  # Apply changes
  python -m scripts.canonicalize_with_policies --apply
"""

import os
import sys
import sqlite3
import argparse
import shutil
import time
from datetime import datetime
from typing import List, Tuple, Dict, Optional, Set

# Ensure project root is on sys.path when run as a plain script
# (…/compliance-os/scripts/canonicalize_with_policies.py -> add parent dir)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.policy.loader import get_policy_for_url  # type: ignore
from src.tools.normalize_policy import suggest_normalized_url  # type: ignore

DB = os.path.join(os.getcwd(), "compliance.db")


def fetch_all_docs(cur) -> List[Tuple[str, Optional[int], int, str, str]]:
    """
    Returns rows as: (url, status_code, body_len, fetched_at, url_original)
    """
    rows = cur.execute(
        """
        SELECT url,
               COALESCE(status_code,0) as sc,
               LENGTH(COALESCE(body,'')) as blen,
               COALESCE(fetched_at,'') as fa,
               COALESCE(url_original,'') as uo
          FROM documents
        """
    ).fetchall()
    return rows


def parse_dt(s: str) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "")).timestamp()
    except Exception:
        # fallback: lexicographic-ish comparison if parse fails
        try:
            return float("".join(ch for ch in s if ch.isdigit()) or 0)
        except Exception:
            return 0.0


def pick_winner(group_rows: List[Tuple[str, int, int, str, str]]) -> int:
    """
    group_rows: list of tuples (url, sc, blen, fa, uo)
    returns index of winner
    """
    best_idx = 0
    best_key = (-1, -1, -1.0)  # (good_status, body_len, ts)
    for i, (_, sc, blen, fa, _) in enumerate(group_rows):
        good = 1 if sc in (200, 304) else 0
        ts = parse_dt(fa)
        key = (good, blen, ts)
        if key > best_key:
            best_key = key
            best_idx = i
    return best_idx


def backup_db(db_path: str) -> str:
    ts = time.strftime("%Y%m%d_%H%M%S")
    dst = db_path + f".bak_{ts}"
    shutil.copy2(db_path, dst)
    return dst


def consolidate_queue(cur, canonical_url: str, urls_in_group: List[str]) -> None:
    """
    Ensure discovery_queue has a single row for canonical_url with the 'best' status.
    Removes any old rows for losers.
    Best status rule: 'hydrated' if any hydrated else the first non-empty status else 'queued'.
    """
    placeholders = ",".join(["?"] * len(urls_in_group))
    try:
        rows = cur.execute(
            f"SELECT url, COALESCE(status,'') FROM discovery_queue WHERE url IN ({placeholders})",
            tuple(urls_in_group),
        ).fetchall()
    except Exception:
        rows = []

    statuses = [s for (_u, s) in rows if s]
    best_status = "hydrated" if "hydrated" in statuses else (statuses[0] if statuses else "queued")

    # Ensure canonical row exists and has best status
    try:
        cur.execute(
            "INSERT OR IGNORE INTO discovery_queue (url, discovered_from, status) VALUES (?, ?, ?)",
            (canonical_url, "canonicalize", best_status),
        )
        cur.execute("UPDATE discovery_queue SET status=? WHERE url=?", (best_status, canonical_url))
    except Exception:
        pass

    # Remove all non-canonical rows for this group
    losers = [u for u in urls_in_group if u != canonical_url]
    for lu in losers:
        try:
            cur.execute("DELETE FROM discovery_queue WHERE url=?", (lu,))
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write changes (default is dry-run)")
    ap.add_argument("--limit", type=int, default=0, help="limit number of canonical groups to process (0 = all)")
    args = ap.parse_args()

    if not os.path.exists(DB):
        print(f"[err] DB not found at: {DB}")
        sys.exit(1)

    con = sqlite3.connect(DB)
    cur = con.cursor()

    rows = fetch_all_docs(cur)
    if not rows:
        print("No documents found.")
        con.close()
        return

    # Build canonical groups
    groups: Dict[str, List[Tuple[str, int, int, str, str]]] = {}
    changes = 0
    for (url, sc, blen, fa, uo) in rows:
        try:
            pol = get_policy_for_url(url)
            canon = suggest_normalized_url(url, pol)  # does not depend on enforcement env var
        except Exception:
            canon = url
        groups.setdefault(canon, []).append((url, sc, blen, fa, uo))
        if canon != url:
            changes += 1

    total_groups = len(groups)
    print(f"[scan] documents={len(rows)}  canonical_groups={total_groups}  urls_to_change={changes}")

    # Show a few duplicate groups in dry-run output
    dup_samples_shown = 0
    for canon, items in groups.items():
        if len(items) <= 1:
            continue
        if dup_samples_shown < 8:
            print("\n[dup-group]")
            print(" canonical:", canon)
            for (u, sc, blen, fa, _uo) in items:
                print(f"   - {u} (sc={sc} len={blen} at={fa})")
            dup_samples_shown += 1

    if not args.apply:
        print("\n[dry-run] No changes applied. Re-run with --apply to write.")
        con.close()
        return

    # Backup before mutating
    bak = backup_db(DB)
    print(f"[backup] created: {bak}")

    processed_groups = 0
    mutated_docs = 0
    deleted_docs = 0
    queue_updates = 0

    for canon, items in groups.items():
        if args.limit and processed_groups >= args.limit:
            break
        processed_groups += 1

        urls_in_group = [u for (u, _sc, _blen, _fa, _uo) in items]

        if len(items) == 1:
            # Single row: if url != canon, rename to canon (or drop if conflict)
            (url, sc, blen, fa, uo) = items[0]
            if url == canon:
                continue

            # Update queue first
            consolidate_queue(cur, canon, [url])
            queue_updates += 1

            # Now update document URL to canonical
            try:
                cur.execute("UPDATE documents SET url=? WHERE url=?", (canon, url))
                mutated_docs += 1
            except sqlite3.IntegrityError:
                # canonical already exists; drop old
                cur.execute("DELETE FROM documents WHERE url=?", (url,))
                deleted_docs += 1

            continue

        # Multiple rows map to same canonical -> pick a winner and merge
        win_idx = pick_winner(items)
        winner = items[win_idx][0]

        # Prefer a winner that already equals canonical to avoid update conflicts
        if winner != canon:
            for j, (u, _sc, _blen, _fa, _uo) in enumerate(items):
                if u == canon:
                    win_idx = j
                    winner = u
                    break

        losers = [u for (u, _sc, _blen, _fa, _uo) in items if u != winner]

        # Update queue to canonical and remove losers from queue
        consolidate_queue(cur, canon, [winner] + losers)
        queue_updates += 1

        # Delete loser documents
        for lu in losers:
            try:
                cur.execute("DELETE FROM documents WHERE url=?", (lu,))
                deleted_docs += 1
            except Exception:
                pass

        # Ensure winner is at canonical URL
        if winner != canon:
            try:
                cur.execute("UPDATE documents SET url=? WHERE url=?", (canon, winner))
                mutated_docs += 1
            except sqlite3.IntegrityError:
                # Canonical row exists already; drop old winner
                cur.execute("DELETE FROM documents WHERE url=?", (winner,))
                deleted_docs += 1

    con.commit()
    con.close()

    print("\n[apply] canonicalization + dedupe complete.")
    print(f" groups processed : {processed_groups if not args.limit else min(processed_groups, args.limit)}")
    print(f" doc rows updated : {mutated_docs}")
    print(f" doc rows deleted : {deleted_docs}")
    print(f" queue updates    : {queue_updates}")
    print("Next: run hydration in --changed-only mode to top-up any canonical rows.")
    print("e.g., python scripts\\hydrate_smart.py --changed-only --skip-assets --pause 0.3 --timeout 25 --render-timeout 25")


if __name__ == "__main__":
    main()
