#!/usr/bin/env python
import os, sys, glob, subprocess, datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
OUT_PATH = os.path.join(ROOT, "pipeline_probe.txt")
PY = sys.executable or "python"

def run_cmd(cmd, timeout=8):
    try:
        p = subprocess.run(
            cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout
        )
        out = (p.stdout or "") + (("\n" + p.stderr) if p.stderr else "")
        return p.returncode, out.strip()
    except Exception as e:
        return -1, f"{e.__class__.__name__}: {e}"

def head_lines(path, n=20):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()[:n]
        return "".join(lines).rstrip()
    except Exception as e:
        return f"(could not read file: {e.__class__.__name__}: {e})"

def main():
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scripts = sorted(
        [os.path.basename(p) for p in glob.glob(os.path.join(SCRIPTS_DIR, "*.py"))]
    )

    # Summary header
    lines = []
    lines.append(f"### PIPELINE PROBE (generated {now})")
    lines.append("")
    lines.append("= =   S C R I P T S   P R E S E N T   = =")
    lines.append("")
    lines.append(f"{'Name':34} {'Size':>8}   {'Modified'}")
    lines.append(f"{'-'*34} {'-'*8}   {'-'*28}")

    for name in scripts:
        fp = os.path.join(SCRIPTS_DIR, name)
        try:
            size = os.path.getsize(fp)
            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            size, mtime = 0, "n/a"
        lines.append(f"{name:34} {size:8d}   {mtime}")

    # Per-script sections with stable markers
    for name in scripts:
        fp = os.path.join(SCRIPTS_DIR, name)
        lines.append("")
        lines.append(f"==== SCRIPT: {name} ====")
        lines.append("")
        lines.append("---- HEAD (first 20 lines) ----")
        lines.append(head_lines(fp, 20))
        lines.append("")
        lines.append("---- HELP ----")
        rc, help_out = run_cmd([PY, os.path.join("scripts", name), "-h"])
        if rc == 0 and help_out.strip():
            lines.append(help_out)
        else:
            # Fallback: run with no args, capture whatever it prints
            rc2, out2 = run_cmd([PY, os.path.join("scripts", name)])
            if (out2 or "").strip():
                lines.append("( !) help failed or empty; showing output with no args")
                lines.append("")
                lines.append(out2)
            else:
                lines.append("( !) help failed and no output with no args")

    with open(OUT_PATH, "w", encoding="utf-8", errors="replace") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Wrote {OUT_PATH} for {len(scripts)} scripts")

if __name__ == "__main__":
    main()
