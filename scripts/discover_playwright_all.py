#!/usr/bin/env python
import os, sys, json, argparse, subprocess, shlex, re

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SEEDS_PATH = os.path.join(ROOT, "config", "seeds.json")
DISCOVER_SCRIPT = os.path.join(HERE, "discover_playwright.py")

URL_RE = re.compile(r"^https?://", re.I)

DEFAULT_TIERS = [
    "social media — tier 1 (global)",
    "social media — tier 2 (alternates & creators)",
    "social media — tier 3 (regional majors)",
    "social media — tier 4 (gaming/ugc communities)",
    "social media — photo/video & misc",
]

def load_seeds(path: str) -> dict:
    if not os.path.exists(path):
        print(f"[ERR] seeds file not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Only keep list values
    return {k: v for k, v in data.items() if isinstance(v, list)}

def valid_urls(items):
    return [s for s in items if isinstance(s, str) and URL_RE.match(s)]

def run_one(root: str, max_urls: int, scrolls: int, timeout_ms: int, headed: bool):
    cmd = [
        sys.executable, DISCOVER_SCRIPT, root,
        "--max", str(max_urls),
        "--scrolls", str(scrolls),
        "--timeout", str(timeout_ms),
    ]
    if headed:
        cmd.append("--headed")

    print(f"\n[run] {root}")
    try:
        proc = subprocess.run(
            cmd,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        print(proc.stdout.rstrip())
        if proc.returncode != 0:
            print(f"[ERR] {root} => exit {proc.returncode}")
    except Exception as e:
        print(f"[ERR] {root} => {e}")

def main():
    ap = argparse.ArgumentParser(description="Batch Playwright discovery by tier keys in seeds.json")
    ap.add_argument(
        "--tiers",
        nargs="*",
        default=DEFAULT_TIERS,
        help="Tier keys to run from seeds.json (default = social media tiers)",
    )
    ap.add_argument("--max", type=int, default=200, help="max URLs to enqueue per root")
    ap.add_argument("--scrolls", type=int, default=10, help="scroll passes per page")
    ap.add_argument("--timeout", type=int, default=20000, help="per-page timeout (ms)")
    ap.add_argument("--headed", action="store_true", help="run browser headed (debug)")
    args = ap.parse_args()

    if not os.path.exists(DISCOVER_SCRIPT):
        print(f"[ERR] missing {DISCOVER_SCRIPT}.")
        sys.exit(1)

    seeds = load_seeds(SEEDS_PATH)
    if not seeds:
        sys.exit(1)

    for key in args.tiers:
        roots = valid_urls(seeds.get(key, []))
        print(f"\n=== Running Playwright discovery for {key} ({len(roots)} site{'s' if len(roots)!=1 else ''}) ===")
        if not roots:
            print(f"[warn] no valid URLs under key: {key}")
            continue
        for root in roots:
            run_one(root, args.max, args.scrolls, args.timeout, args.headed)

if __name__ == "__main__":
    main()
