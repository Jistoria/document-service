import re
import unicodedata
from typing import List, Optional, Tuple

EMAIL_REGEX = re.compile(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)")

TITLE_PREFIXES = {"ing", "msc", "dr", "dra", "lic", "abg", "sr", "sra", "prof", "phd"}

LABEL_PATTERNS = [
    r"nombres\s+del\s+tutor\s+instituci[oó]n\s+receptora\s*:\s*",
    r"tutor\s+acad[eé]mico\s*:\s*",
    r"tutor\s*:\s*",
    r"autor\s*\(estudiante\)\s*:\s*",
    r"autor\s*:\s*",
    r"author\s*:\s*",
]

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _clean_base(raw: str) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return None, None

    s = str(raw).strip()

    email_match = EMAIL_REGEX.search(s)
    email = email_match.group(1) if email_match else None

    cleaned = s
    for pat in LABEL_PATTERNS:
        cleaned = re.sub(pat, "", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # quitar títulos al inicio (Ing., Dr., etc.)
    tokens = cleaned.split()
    while tokens:
        t = tokens[0].lower().rstrip(".")
        if t in TITLE_PREFIXES:
            tokens.pop(0)
        else:
            break
    cleaned = " ".join(tokens).strip()

    if len(cleaned) < 3:
        cleaned = None

    return cleaned, email


def build_search_terms(raw: str) -> Tuple[Optional[str], Optional[str], dict]:
    """
    Retorna:
      - name (con acentos como viene)
      - email
      - parts dict: {first, first2, last, last_first, full_ascii}
    """
    name, email = _clean_base(raw)
    if not name and not email:
        return None, None, {}

    parts = {}
    if name:
        ascii_name = _strip_accents(name)
        tokens = [t for t in re.split(r"\s+", ascii_name) if t]
        if tokens:
            parts["first"] = tokens[0]
            parts["last"] = tokens[-1]
        if len(tokens) >= 2:
            parts["first2"] = f"{tokens[0]} {tokens[1]}"
            parts["last_first"] = f"{tokens[-1]} {tokens[0]}"
        parts["full_ascii"] = ascii_name

    if email:
        parts["email_prefix"] = email.split("@")[0]

    return name, email, parts
