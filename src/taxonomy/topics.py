import re

TOPIC_PATTERNS = {
    "medical-claims": re.compile(r"\b(cure|treats?|disease|FDA|clinical|symptom)\b", re.I),
    "weight-loss": re.compile(r"\b(weight\s*loss|fat\s*burn|before\s*and\s*after)\b", re.I),
    "counterfeit": re.compile(r"\b(counterfeit|fake|replica)\b", re.I),
    "adult-content": re.compile(r"\b(explicit|porn|sexual)\b", re.I),
    "political-ads": re.compile(r"\b(political\s*ad|election|campaign)\b", re.I),
    "privacy": re.compile(r"\b(personal\s*data|consent|cookie|GDPR|CCPA)\b", re.I)
}

def tag_topics(text: str):
    tags = []
    for name, rx in TOPIC_PATTERNS.items():
        if rx.search(text):
            tags.append(name)
    return sorted(set(tags))
