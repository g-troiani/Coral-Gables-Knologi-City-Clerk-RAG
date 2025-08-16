from __future__ import annotations
from datetime import datetime
from typing import Optional
import re

# Accept common inputs like '01.09.2024', '1-9-2024', '2024/01/09', '2024-1-9', etc.

def to_yyyy_mm_dd(s: str | None) -> str:
    """Return 'YYYY_MM_DD' or 'unknown' if not parseable."""
    if not s:
        return "unknown"
    t = (s or "").strip().replace("/", "-").replace(".", "-").replace("_", "-")
    for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(t, fmt)
            return f"{dt.year:04d}_{dt.month:02d}_{dt.day:02d}"
        except ValueError:
            pass
    # fallback: keep digits, then try to reshape if looks like YYYYMMDD
    digits = re.sub(r"\D", "", t)
    if len(digits) == 8 and digits[:4].isdigit():
        return f"{digits[:4]}_{digits[4:6]}_{digits[6:]}"
    return "unknown"

def canon_yyyymmdd(s: str | None) -> str:
    """Return 'YYYYMMDD' or '' if not parseable."""
    if not s:
        return ""
    ymd = to_yyyy_mm_dd(s)
    return "" if ymd == "unknown" else ymd.replace("_", "")

def to_iso8601_z(s: str | None) -> Optional[str]:
    """Return 'YYYY-MM-DDT00:00:00Z' for a date, else None."""
    if not s:
        return None
    t = (s or "").strip().replace("/", "-").replace(".", "-").replace("_", "-")
    for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(t, fmt)
            return dt.strftime("%Y-%m-%dT00:00:00Z")
        except ValueError:
            pass
    digits = re.sub(r"\D", "", t)
    if len(digits) == 8 and digits[:4].isdigit():
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}T00:00:00Z"
    return None
