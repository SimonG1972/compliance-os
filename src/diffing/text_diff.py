import difflib, json
from pathlib import Path
from ..config import DIFF_DIR

def make_diff(old_text: str, new_text: str) -> str:
    diff = list(difflib.unified_diff(
        old_text.splitlines(), new_text.splitlines(),
        fromfile="old", tofile="new", lineterm=""
    ))
    content = "\n".join(diff)
    h = abs(hash(content))
    path = DIFF_DIR / f"diff_{h}.patch"
    Path(path).write_text(content)
    return str(path)

def as_text(norm_doc: dict) -> str:
    return "\n\n".join([f"# {s['title']}\n{s['text']}" for s in norm_doc["sections"]])
