from __future__ import annotations

from dataclasses import dataclass, field
from typing import List
import re
import os

try:
    import yaml  # PyYAML
except Exception as e:
    raise RuntimeError(
        "PyYAML is required. Install with: pip install pyyaml"
    ) from e


LEGAL_HINTS = [
    r"/privacy(\b|-|/)", r"/legal(\b|-|/)", r"/terms(\b|-|/)",
    r"/policy(\b|-|/)", r"/policies(\b|-|/)", r"/community(\b|-|/)",
    r"/guidelines(\b|-|/)", r"/help(\b|-|/).*policy", r"/support(\b|-|/).*policy",
    r"/ads(\b|-|/)", r"/advertising(\b|-|/)", r"/safety(\b|-|/)", r"/security(\b|-|/)",
    r"/compliance(\b|-|/)", r"/cookie(\b|-|/)", r"/cookies(\b|-|/)",
    r"/data(\b|-|/).*retention", r"/children(\b|-|/)", r"/youth(\b|-|/)",
    r"/dmca(\b|-|/)", r"/copyright(\b|-|/)", r"/ip(\b|-|/)",
]
LEGAL_RX = re.compile("|".join(LEGAL_HINTS), re.IGNORECASE)


def looks_like_legal(url: str) -> bool:
    """Heuristic: does a URL look like a policy/legal/support doc?"""
    if not url:
        return False
    if LEGAL_RX.search(url):
        return True
    # Short allow for known doc roots
    if any(url.lower().endswith(sfx) for sfx in ("/legal", "/policies", "/policy")):
        return True
    return False


@dataclass
class Entity:
    name: str
    category: str
    home: str
    hubs: List[str] = field(default_factory=list)
    sitemaps: List[str] = field(default_factory=list)
    js_required: bool = False
    depth: str = "shallow"      # shallow | hub | sitemap
    priority: int = 5
    jurisdictions: List[str] = field(default_factory=lambda: ["global"])
    notes: str = ""


def _coerce_entity(raw: dict) -> Entity:
    return Entity(
        name=raw.get("name", "").strip(),
        category=(raw.get("category") or "other").strip(),
        home=raw.get("home", "").strip(),
        hubs=[h.strip() for h in (raw.get("hubs") or []) if h and isinstance(h, str)],
        sitemaps=[s.strip() for s in (raw.get("sitemaps") or []) if s and isinstance(s, str)],
        js_required=bool(raw.get("js_required", False)),
        depth=(raw.get("depth") or "shallow").strip(),
        priority=int(raw.get("priority", 5)),
        jurisdictions=[j.strip() for j in (raw.get("jurisdictions") or ["global"]) if j and isinstance(j, str)],
        notes=(raw.get("notes") or "").strip(),
    )


def load_catalog(path: str = "seeds/catalog.yaml") -> List[Entity]:
    """Load entities list from YAML."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or []
    if not isinstance(data, list):
        raise ValueError(f"Catalog YAML must be a list of entities; got {type(data)}")
    entities = [_coerce_entity(item) for item in data]
    # Keep only minimal valid rows
    entities = [e for e in entities if e.name and e.home]
    return entities
