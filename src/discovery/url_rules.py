import re

# Terms we care about, both for discovery and lightweight classification
KEYWORDS = {
    "tos": [
        r"\bterms\b", r"\bterms\s+of\s+(use|service)\b", r"\buser\s+agreement\b",
        r"\bcommunity\s+guidelines?\b", r"\bcommunity\s+standards?\b"
    ],
    "privacy": [r"\bprivacy\b", r"\bdata\s+protection\b", r"\bpersonal\s+data\b"],
    "ads": [r"\badvertis(ing|ement)s?\b", r"\bendorsements?\b", r"\bpaid\s+partnerships?\b"],
    "commerce": [r"\bprohibited\s+items?\b", r"\bcounterfeit\b", r"\breturns?\b", r"\brefunds?\b"],
    "regulation": [r"\bcode\b", r"\bsection\s+\d+", r"\bCFR\b", r"\bregulation(s)?\b", r"\blaw\b"],
    "developer": [r"\bapi\b", r"\bplatform\s+policy\b", r"\bdeveloper\b"],
    "appeals": [r"\bappeal(s)?\b", r"\baccount\s+status\b", r"\bviolations?\b", r"\bmoderation\b"]
}

# Footer link text we’ll consider “likely legal”
FOOTER_HINTS = re.compile(
    r"(terms|privacy|policy|policies|legal|guidelines|safety|trust|ads|advertising|help|support)",
    re.I
)

# Paths that often host policies
LIKELY_PATHS = [
    "/legal", "/policies", "/policy", "/terms", "/terms-of-service", "/terms-of-use",
    "/privacy", "/privacy-policy", "/community-guidelines", "/help/policies"
]

def classify(title: str, text: str) -> tuple[str|None, float]:
    """
    Very light classifier: returns (doc_type, confidence).
    Confidence is proportional to number of keyword hits normalized by text size.
    """
    hay = f"{title}\n{text}".lower()
    best = (None, 0.0)
    for label, patterns in KEYWORDS.items():
        score = 0
        for pat in patterns:
            score += len(re.findall(pat, hay, re.I))
        # normalize a bit by doc length
        denom = max(500, len(hay))
        conf = min(1.0, score * 200 / denom)  # generous scaling
        if conf > best[1]:
            best = (label, conf)
    return best
