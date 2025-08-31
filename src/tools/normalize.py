"""
URL normalization helper.

Ensures we always store a canonical version of URLs in the DB,
while preserving the original string for provenance.

Rules:
- lowercase host
- drop default ports (:80, :443)
- strip fragments (#…)
- remove tracking params (utm_*, gclid, fbclid, mc_eid, igshid, etc.)
- collapse www.
- canonicalize trailing slashes (remove unless it's the root '/')
"""

from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

TRACKING_PARAMS = {"utm_", "gclid", "fbclid", "mc_eid", "igshid"}

def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)

        # lowercase host
        netloc = parsed.hostname.lower() if parsed.hostname else ""
        if parsed.port:
            if not ((parsed.scheme == "http" and parsed.port == 80) or
                    (parsed.scheme == "https" and parsed.port == 443)):
                netloc += f":{parsed.port}"

        # collapse www
        if netloc.startswith("www."):
            netloc = netloc[4:]

        # strip fragment
        fragment = ""

        # remove tracking params
        qparams = []
        for k, v in parse_qsl(parsed.query, keep_blank_values=True):
            if any(k.lower().startswith(tp) for tp in TRACKING_PARAMS):
                continue
            qparams.append((k, v))
        query = urlencode(qparams)

        # canonicalize trailing slash
        path = parsed.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        normalized = urlunparse((
            parsed.scheme.lower(),
            netloc,
            path,
            parsed.params,
            query,
            fragment
        ))
        return normalized
    except Exception:
        # if parsing fails, return original
        return url
