from pathlib import Path
import json, os, subprocess, sys
from shutil import copytree, copy2

def run(cmd: list[str], cwd: Path, env: dict):
    # use venv's interpreter and ensure repo root is on PYTHONPATH
    res = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        print("STDOUT:\n", res.stdout)
        print("STDERR:\n", res.stderr)
    assert res.returncode == 0

def test_end_to_end_offline(tmp_path):
    # repo root (where tests/ lives)
    repo = Path(__file__).resolve().parents[1]

    # working dir is an empty temp sandbox
    work = Path(tmp_path)
    os.chdir(work)

    # minimal tree the CLI expects
    (work / "data/raw").mkdir(parents=True, exist_ok=True)
    (work / "data/normalized").mkdir(parents=True, exist_ok=True)
    (work / "data/diffs").mkdir(parents=True, exist_ok=True)
    (work / "schemas").mkdir(parents=True, exist_ok=True)
    (work / "tests/fixtures").mkdir(parents=True, exist_ok=True)

    # copy schemas and the offline HTML fixture into the sandbox
    copytree(repo / "schemas", work / "schemas", dirs_exist_ok=True)
    copy2(
        repo / "tests/fixtures/platform_tos_example.html",
        work / "tests/fixtures/platform_tos_example.html",
    )

    # local sources.yaml pointing at the file:// fixture
    (work / "data/sources.yaml").write_text(
        """\
- source_name: Fixture Example
  platform_or_regulator: Test
  doc_type: tos
  url: file://./tests/fixtures/platform_tos_example.html
  jurisdiction: test
  volatility: low
"""
    )

    # Build environment so the CLI (in repo root) is importable from temp cwd
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo)  # <-- make 'src' resolvable

    py = sys.executable  # use the current venv's Python

    # init + load + fetch + index
    run([py, "-m", "src.cli", "init"], cwd=work, env=env)
    run([py, "-m", "src.cli", "load-sources"], cwd=work, env=env)
    run([py, "-m", "src.cli", "fetch-all"], cwd=work, env=env)

    # verify a normalized doc exists
    norm = list((work / "data/normalized").glob("*.json"))
    assert len(norm) >= 1
    doc = json.loads(norm[0].read_text(encoding="utf-8"))
    assert doc["doc_type"] == "tos"
    assert "sections" in doc and len(doc["sections"]) >= 1

    # index + search
    run([py, "-m", "src.cli", "reindex"], cwd=work, env=env)
    out = subprocess.check_output(
        [py, "-m", "src.cli", "search", "terms OR privacy"], cwd=work, env=env, text=True
    )
    assert "Fixture Example" in out
