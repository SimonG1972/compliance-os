from datetime import datetime
import hashlib, json
from pathlib import Path
from ..taxonomy.topics import tag_topics
from ..config import NORM_DIR

def normalize(doc_meta: dict, sections: list, raw_path: str) -> dict:
    text_blob = "\n\n".join([f"# {s['title']}\n{s['text']}" for s in sections])
    h = hashlib.blake2b(text_blob.encode("utf-8"), digest_size=20).hexdigest()
    norm = {
        "source_name": doc_meta["source_name"],
        "platform_or_regulator": doc_meta.get("platform_or_regulator"),
        "doc_type": doc_meta["doc_type"],
        "url": doc_meta["url"],
        "jurisdiction": doc_meta.get("jurisdiction","global"),
        "effective_date": None,
        "version_date": datetime.utcnow().isoformat(),
        "topics": tag_topics(text_blob),
        "sections": sections,
        "penalties": [],
        "appeals": "",
        "raw_path": raw_path,
        "hash": h
    }
    out = NORM_DIR / f"{h}.json"
    out.write_text(json.dumps(norm, ensure_ascii=False, indent=2))
    return norm, str(out)
