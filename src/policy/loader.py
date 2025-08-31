# src/policy/loader.py
import os
import glob
from typing import Dict, Optional

import yaml

from .model import Policy

_CACHE: Dict[str, Policy] = {}
_ALIAS: Dict[str, str] = {}
_ENFORCE: Optional[bool] = None  # cached


def _truthy(val: Optional[str]) -> bool:
    return bool(val and val.strip().lower() in ("1", "true", "yes", "on"))


def policy_enforce_enabled() -> bool:
    """Read once from env COMPLIANCEOS_POLICY_ENFORCE and cache."""
    global _ENFORCE
    if _ENFORCE is None:
        _ENFORCE = _truthy(os.environ.get("COMPLIANCEOS_POLICY_ENFORCE", ""))
    return bool(_ENFORCE)


def _load_all() -> None:
    """Load all YAML policies from config/policies/*.yml into memory once."""
    if _CACHE:
        return
    path = os.path.join("config", "policies", "*.yml")
    for f in glob.glob(path):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        except Exception:
            data = {}
        try:
            p = Policy(**data)
        except Exception:
            # If a policy file is malformed, skip it rather than breaking runs.
            continue
        _CACHE[p.host] = p
        for a in p.aliases:
            _ALIAS[a] = p.host

    if "default" not in _CACHE:
        _CACHE["default"] = Policy(host="default")


def get_policy_for_host(host: str) -> Policy:
    _load_all()
    key = _ALIAS.get(host, host)
    return _CACHE.get(key, _CACHE["default"])


def get_policy_for_url(url: str) -> Policy:
    from urllib.parse import urlparse
    host = (urlparse(url).hostname or "").lower()
    return get_policy_for_host(host)
