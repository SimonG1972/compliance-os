# src/tools/normalize_policy.py
import re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from src.policy.model import Policy
from src.policy.loader import policy_enforce_enabled

def _strip_query_params(query: str, strip_patterns: list) -> str:
    if not strip_patterns: return query
    drop = [p.replace("*", ".*") for p in strip_patterns]
    keep = []
    for k, v in parse_qsl(query, keep_blank_values=True):
        if any(re.fullmatch(p, k) for p in drop):
            continue
        keep.append((k, v))
    return urlencode(keep, doseq=True)

def suggest_normalized_url(url: str, policy: Policy) -> str:
    u = urlparse(url)
    host = u.hostname or ""
    if policy.normalization.collapse_www and host.startswith("www."):
        host = host[4:]
    netloc = host
    if u.port and not (u.port in (80, 443) and policy.normalization.default_port_strip):
        netloc = f"{host}:{u.port}"
    fragment = "" if policy.normalization.drop_fragments else u.fragment
    query = _strip_query_params(u.query, policy.normalization.strip_params)
    return urlunparse((u.scheme, netloc, u.path, "", query, fragment))

def allowed_by_policy_path(url: str, policy: Policy) -> bool:
    p = (urlparse(url).path or "/")
    allow = policy.normalization.path_allow_prefixes or []
    deny  = policy.normalization.path_deny_regexes or []
    if allow and not any(p.startswith(pref) for pref in allow):
        return False
    for rx in deny:
        if re.search(rx, p):
            return False
    return True

def normalize_url_with_policy(url: str, policy: Policy) -> str:
    """
    Shadow-safe: if enforcement is OFF, return url unchanged.
    If ON, return suggested normalization.
    """
    if not policy_enforce_enabled():
        return url  # shadow mode (no behavior change)
    return suggest_normalized_url(url, policy)
