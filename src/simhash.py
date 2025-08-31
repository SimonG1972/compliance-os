import re
from collections import Counter
from hashlib import blake2b

_WORD = re.compile(r"[A-Za-z0-9_]+", re.U)

def _tokens(text, n=3):
    toks = _WORD.findall(text.lower())
    if n <= 1:
        return toks
    return [" ".join(toks[i:i+n]) for i in range(max(1, len(toks)-n+1))]

def simhash_bits(text: str, ngram=3, bits=128) -> int:
    # Lightweight SimHash using blake2b to get consistent per-token vectors
    if not text:
        return 0
    V = [0]*bits
    counts = Counter(_tokens(text, n=ngram))
    for tok, w in counts.items():
        h = blake2b(tok.encode("utf-8"), digest_size=bits//8).digest()
        for i, byte in enumerate(h):
            for b in range(8):
                bitpos = i*8 + b
                if bitpos >= bits: break
                if (byte >> (7-b)) & 1:
                    V[bitpos] += w
                else:
                    V[bitpos] -= w
    x = 0
    for i, v in enumerate(V):
        if v >= 0:
            x |= (1 << (bits-1-i))
    return x

def simhash_hex(text: str, ngram=3, bits=128) -> str:
    val = simhash_bits(text, ngram=ngram, bits=bits)
    width = bits // 4  # hex digits
    return format(val, f"0{width}x")
