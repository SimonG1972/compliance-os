# src/tools/tagging.py
from __future__ import annotations
import re
from urllib.parse import urlparse

DOC_TYPE_RULES = [
    (r"privacy", "privacy"),
    (r"terms|tos|terms-of-service", "terms"),
    (r"guidelines|community|rules|standards|code-of-conduct", "guidelines"),
    (r"ads|advertis", "ads"),
    (r"policy|policies|legal", "policy"),
    (r"children|minors|kid|youth|under\s*13", "children"),
    (r"copyright|dmca|ip|intellectual[- ]property", "ip"),
    (r"cookie", "cookie"),
    (r"moderation|enforcement|safety|abuse|harassment", "safety"),
]

def infer_doc_type(url: str, title: str = "") -> str:
    s = f"{url} {title}".lower()
    for pat, label in DOC_TYPE_RULES:
        if re.search(pat, s):
            return label
    return "other"

def infer_platform(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    base = host.split(":")[0]
    parts = base.split(".")
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return base.capitalize() or "Unknown"

def infer_jurisdiction(url: str) -> str:
    path = urlparse(url).path.lower()
    if any(tok in path for tok in ["/eu", "/en-eu", "/europe"]):
        return "eu"
    if any(tok in path for tok in ["/uk", "/en-gb"]):
        return "uk"
    if any(tok in path for tok in ["/us", "/en-us"]):
        return "us"
    return "global"
