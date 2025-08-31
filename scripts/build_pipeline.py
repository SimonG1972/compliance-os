#!/usr/bin/env python
import os, re, json, sys, glob

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
PROBE_PATH = os.path.join(ROOT, "pipeline_probe.txt")
PY = sys.executable or "python"

def parse_from_probe(probe_path):
    if not os.path.exists(probe_path):
        return []
    with open(probe_path, encoding="utf-8", errors="ignore") as f:
        text = f.read()

    # Preferred marker produced by pipeline_probe.py:
    # "==== SCRIPT: alert_changes.py ===="
    names = re.findall(r"^=+\s*SCRIPT\s*:\s*([^\s=]+)\s*=+\s*$", text, flags=re.M)

    # Fallback: grab any ".py" token on lines and keep only those that actually exist in /scripts
    if not names:
        existing = set(os.listdir(SCRIPTS_DIR))
        candidates = re.findall(r"([A-Za-z0-9_][\w\-.]+\.py)", text)
        seen = set()
        names = [n for n in candidates if n in existing and not (n in seen or seen.add(n))]

    return names

def list_from_dir():
    # If probe parse fails, just enumerate the directory.
    exclude = {"__init__.py", "build_pipeline.py", "pipeline_probe.py"}
    names = [os.path.basename(p) for p in glob.glob(os.path.join(SCRIPTS_DIR, "*.py"))]
    names = [n for n in names if n not in exclude]
    return sorted(names)

def main():
    names = parse_from_probe(PROBE_PATH)
    if not names:
        names = list_from_dir()

    scripts = []
    for n in names:
        # Use the current interpreter so venvs work; keep a relative script path.
        cmd = f"{PY} {os.path.join('scripts', n)}"
        scripts.append({"name": n, "command": cmd})

    out_path = os.path.join(ROOT, "pipeline.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(scripts, f, indent=2)
    print(f"Wrote {os.path.basename(out_path)} with {len(scripts)} scripts")

if __name__ == "__main__":
    main()
