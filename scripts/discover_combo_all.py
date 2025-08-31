#!/usr/bin/env python
import os, sys, json, time, argparse, subprocess

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPTS = os.path.join(ROOT, "scripts")
SEEDS = os.path.join(ROOT, "config", "seeds.json")

CANDIDATE_KEYS = [
    # short keys
    "social-tier-1","social-tier-2","social-tier-3","social-tier-4","social-photo-misc",
    # long names (your file has both)
    "social media — tier 1 (global)",
    "social media — tier 2 (alternates & creators)",
    "social media — tier 3 (regional majors)",
    "social media — tier 4 (gaming/ugc communities)",
    "social media — photo/video & misc",
]

def run(cmd:list) -> int:
    p = subprocess.run(cmd, cwd=ROOT)
    return p.returncode

def main():
    ap = argparse.ArgumentParser(description="Run discover_combo.py across social tiers from seeds.json")
    ap.add_argument("--keys", default=",".join(CANDIDATE_KEYS), help="comma-separated keys to include (will skip missing)")
    ap.add_argument("--max", type=int, default=300)
    ap.add_argument("--scrolls", type=int, default=12)
    ap.add_argument("--timeout", type=int, default=30000)
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--force-js", action="store_true")
    ap.add_argument("--hubs", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.5)
    args = ap.parse_args()

    if not os.path.exists(SEEDS):
        print(f"[ERR] seeds file not found: {SEEDS}")
        sys.exit(2)

    with open(SEEDS, "r", encoding="utf-8") as f:
        seeds = json.load(f)

    wanted = [k.strip() for k in args.keys.split(",") if k.strip()]
    roots = []
    for k in wanted:
        if k in seeds and isinstance(seeds[k], list):
            roots += [u for u in seeds[k] if isinstance(u, str) and u.startswith("http")]
        else:
            # soft fallback: case-insensitive contains
            for kk, vv in seeds.items():
                if k.lower() in kk.lower() and isinstance(vv, list):
                    roots += [u for u in vv if isinstance(u, str) and u.startswith("http")]
    # unique, keep order
    seen = set(); uniq = []
    for u in roots:
        if u not in seen:
            seen.add(u); uniq.append(u)

    tiers_label = ", ".join(wanted)
    print(f"\n=== Running COMBO discovery for {tiers_label} ({len(uniq)} sites) ===\n")

    for r in uniq:
        print(f"\n[site] {r}")
        cmd = [
            sys.executable, os.path.join(SCRIPTS, "discover_combo.py"),
            r, "--max", str(args.max),
            "--scrolls", str(args.scrolls),
            "--timeout", str(args.timeout),
        ]
        if args.headed: cmd.append("--headed")
        if args.force_js: cmd.append("--force-js")
        if args.hubs: cmd.append("--hubs")
        rc = run(cmd)
        time.sleep(args.sleep)

    print("\n[done] combo discovery complete.")
    sys.exit(0)

if __name__ == "__main__":
    main()
