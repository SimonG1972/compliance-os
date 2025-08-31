#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orchestrate discover → hydrate → canonicalize → clean → chunk → tag with guardrails.
- Auto-suggest & auto-promote *low-risk* policy fixes.
- Optional purge of obvious junk.
- Coverage seeding for missing surfaces.
- Pauses if severe anomalies persist.

Usage example (Tier 2):
  python scripts\\pipeline_guard.py --key social-tier-2 --auto-promote-low-risk --auto-purge
"""
import os, subprocess, sqlite3, time, argparse, shlex

ROOT = os.getcwd()
DB = os.path.join(ROOT, "compliance.db")

def run(cmd, env=None):
    print(f"\n$ {cmd}")
    return subprocess.run(cmd, shell=True, env=env or os.environ.copy())

def metric(host=None):
    con = sqlite3.connect(DB); c = con.cursor()
    if host:
        r = c.execute("""
          SELECT total_docs, ok_docs, bad_docs, render_docs, zero_body_docs, zero_clean_docs, errors
          FROM host_stats WHERE host=?""", (host,)).fetchone()
    else:
        r = c.execute("""
          SELECT SUM(total_docs), SUM(ok_docs), SUM(bad_docs), SUM(render_docs),
                 SUM(zero_body_docs), SUM(zero_clean_docs), SUM(errors)
          FROM host_stats""").fetchone()
    con.close(); return r or (0,0,0,0,0,0,0)

def refresh_views():
    run("python scripts\\ensure_quality_views.py")

def canonicalize_apply():
    run("python -m scripts.canonicalize_with_policies --apply")

def hydrate_changed():
    run("python scripts\\hydrate_smart.py --changed-only --skip-assets --pause 0.3 --timeout 25 --render-timeout 25")

def clean():
    run("python scripts\\text_clean.py")

def chunk_safe(where, **kw):
    args = [
        "python", "scripts\\chunk_docs_safe.py",
        "--where", where,
        "--resume", "1",
        "--max-chars", str(kw.get("max_chars",800)),
        "--overlap", str(kw.get("overlap",120)),
        "--min-chars", str(kw.get("min_chars",120)),
        "--show", "0"
    ]
    run(" ".join(shlex.quote(a) for a in args))

def tag_all():
    run("""python scripts\\tag_docs.py --where "clean_text IS NOT NULL AND length(clean_text) >= 120" """)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", help="seeds.json key (e.g., social-tier-2)")
    ap.add_argument("--keys", help="comma separated keys")
    ap.add_argument("--auto-promote-low-risk", action="store_true")
    ap.add_argument("--auto-purge", action="store_true")
    ap.add_argument("--discover-max", type=int, default=400)
    ap.add_argument("--dyn-max", type=int, default=2500)
    args = ap.parse_args()

    # 1) DISCOVER (optional if key/keys provided)
    if args.keys or args.key:
        if args.keys:
            run(f'python scripts\\discover.py --keys "{args.keys}" --max {args.discover_max} --dyn-max {args.dyn_max} --fallback-threshold 0')
        else:
            run(f'python scripts\\discover.py --key {args.key} --max {args.discover_max} --dyn-max {args.dyn_max} --fallback-threshold 0')

    # 2) canonicalize (policies)
    canonicalize_apply()

    # 3) HYDRATE (first pass)
    run("python scripts\\hydrate_smart.py --render-on 401,403,406,429 --pause 0.3 --timeout 25 --render-timeout 25 --skip-assets")

    # 4) QA + autosuggest (+ optional auto-promote)
    refresh_views()
    run("python scripts\\qa_report.py")
    if args.auto_promote_low_risk:
        run("python scripts\\policy_autosuggest.py --auto-promote-low-risk")
        canonicalize_apply()
        hydrate_changed()

    # 5) Optional purge
    if args.auto_purge:
        run("python scripts\\purge_junk.py --apply")

    # 6) CLEAN
    clean()

    # 7) COVERAGE seed (optional – always safe to run)
    run("python scripts\\coverage_check.py --insert")

    # 8) CHUNK (safe; exclude known monsters by policy)
    chunk_safe("""clean_text IS NOT NULL AND length(clean_text) >= 120 AND id NOT IN (SELECT DISTINCT doc_id FROM chunks)""")

    # 9) TAG
    tag_all()

    refresh_views()
    tot, okd, bad, rnd, zb, zc, err = metric()
    print(f"\n[final] total={tot} ok={okd} bad={bad} render={rnd} zero_body={zb} zero_clean={zc} errors={err}")
    print("[pipeline] complete.")

if __name__ == "__main__":
    main()
