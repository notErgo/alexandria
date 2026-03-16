"""URL canonicalization and near-duplicate detection.

canonical_url: normalize URL for consistent hashing.
simhash_text: 64-bit simhash of document text (stdlib only, no external deps).
hamming_distance: bit-level distance between two 64-bit integers.
is_near_duplicate: threshold check.
"""
import hashlib
import re
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

# Tracking params to strip (utm_* matched by prefix; rest are exact).
_TRACKING_EXACT = frozenset({
    'fbclid', 'gclid', 'ref', 'source', '_hsenc', '_hsmi',
})

_UTM_PREFIX = 'utm_'


def canonical_url(url: str) -> str:
    """Normalize a URL for consistent dedup hashing.

    Steps:
      - Lowercase scheme and host.
      - Remove tracking query params (utm_*, fbclid, gclid, ref, source, _hsenc, _hsmi).
      - Preserve path as-is (trailing slashes are kept because some servers, e.g.
        hivedigitaltechnologies.com, return different content for /slug vs /slug/).
      - Rebuild with urlunparse.
    Returns empty string for empty/invalid input.
    """
    if not url or not url.strip():
        return ''
    try:
        parsed = urlparse(url)
    except Exception:
        return ''

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()

    # Strip tracking params
    params = [
        (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in _TRACKING_EXACT
        and not k.lower().startswith(_UTM_PREFIX)
    ]
    query = urlencode(params)

    return urlunparse((scheme, netloc, parsed.path, parsed.params, query, ''))


def simhash_text(text: str) -> int:
    """Compute a deterministic 64-bit simhash of the given text.

    Uses MD5 for token hashing (stdlib only, no external deps) to ensure
    consistent results across processes regardless of PYTHONHASHSEED.

    Algorithm:
      1. Tokenize: split into words, lowercase, strip non-alpha-numeric.
      2. Hash each token to 64 bits via MD5 (first 8 bytes, little-endian).
      3. Accumulate a 64-element bit-weight vector.
      4. Build result: bit i is set if weight[i] > 0.
    """
    tokens = re.sub(r'[^a-zA-Z0-9\s]', '', text.lower()).split()
    weights = [0] * 64
    for token in tokens:
        digest = hashlib.md5(token.encode(), usedforsecurity=False).digest()
        token_hash = int.from_bytes(digest[:8], 'little')
        for i in range(64):
            if token_hash & (1 << i):
                weights[i] += 1
            else:
                weights[i] -= 1
    result = 0
    for i in range(64):
        if weights[i] > 0:
            result |= (1 << i)
    return result


def hamming_distance(h1: int, h2: int) -> int:
    """Count differing bits between two 64-bit integers."""
    return bin(h1 ^ h2).count('1')


def is_near_duplicate(h1: int, h2: int, threshold: int = 3) -> bool:
    """Return True if hamming distance between h1 and h2 is within threshold."""
    return hamming_distance(h1, h2) <= threshold
