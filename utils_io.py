"""### utils_io.py
Shared I/O and normalization utilities for the NBIM reconciliation tools.

Features:
- Delimiter detection: supports semicolon-delimited CSVs out of the box.
- Locale-aware numeric parsing: handles '1,23' vs '1.23', thousand separators, spaces.
- Date parsing with automatic day-first inference per column.
- Column name normalization (synonyms) to unify key headers across vendors.
- Helpers to identify date/money/share/rate columns.

IMPORTANT:
- Canonical join keys in this project are: COAC_EVENT_KEY and BANK_ACCOUNTS
  (plural is used here to match the business mapping; strict module resolves
  aliases in both directions so either singular/plural is accepted).
-----------------------------------------------------------------------------"""

from __future__ import annotations
import re
from pathlib import Path
from typing import Iterable, Tuple, Optional, List
import pandas as pd
import numpy as np
from dateutil import parser as dateparser

# -----------------------
# Schema & column helpers
# -----------------------
# Canonical keys (used after normalization across the codebase)
KEY_COAC = "COAC_EVENT_KEY"
KEY_BANK = "BANK_ACCOUNTS"  # canonical, plural to match the requested spec

def is_skip_compare_col(name: str) -> bool:
    """Return True for columns that should never be compared (e.g., custodian display names).
    
    This helper is available to any strict comparison logic that wants to skip
    purely informational name columns. The current strict module uses an
    explicit mapping, so this is not invoked there, but we keep it here for
    completeness and possible reuse.
    """
    n = (name or "").lower()
    if "custodian" in n:
        return True
    if "custody" in n and "name" in n:
        return True
    if "custody bank" in n:
        return True
    return False

# Map of known synonyms (case-insensitive) to canonical names
# This ensures that regardless of vendor header variants, we can restore
# the expected canonical key names (COAC_EVENT_KEY / BANK_ACCOUNTS).
SYNONYMS = {
    "coac_event_key": KEY_COAC,
    "coac key": KEY_COAC,
    "event_key": KEY_COAC,
    "event id": KEY_COAC,
    "bank_account": KEY_BANK,
    "bank accounts": KEY_BANK,
    "bank_accounts": KEY_BANK,
    "bank acct": KEY_BANK,
    "acct": KEY_BANK,
    "account": KEY_BANK,
}

# Heuristic buckets by column name (case-insensitive substring checks)
DATE_HINTS = ("date", "ex_", "ex-", "exdate", "ex date", "payment", "payment_date", "pay_date", "pay date", "record")
MONEY_HINTS = ("amount", "net", "gross", "cash", "tax", "fee", "dividend")
SHARE_HINTS = ("share", "qty", "quantity", "units")
RATE_HINTS = ("fx", "rate", "pct", "percent")

def normalize_colnames(cols: Iterable[str]) -> List[str]:
    """Normalize column names by applying the SYNONYMS map (case-insensitive).
    
    Any non-recognized column is returned unchanged.
    """
    out = []
    for c in cols:
        if c is None:
            out.append("")
            continue
        s = str(c).strip()
        k = s.lower().strip()
        out.append(SYNONYMS.get(k, s))
    return out

# -----------------------
# Date parsing
# -----------------------
def _infer_dayfirst(series: pd.Series) -> bool:
    """Infer day-first format for a date column by sampling ambiguous entries.
    
    Heuristic:
    - Among values like DD/MM/YYYY vs MM/DD/YYYY, if more than ~20% of ambiguous
      samples look like day>12 in the first position, we assume day-first.
    """
    samples = series.dropna().astype(str).head(200).tolist()
    ambiguous = 0
    day_gt_12 = 0
    for s in samples:
        if re.search(r"\b\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{2,4}\b", s):
            parts = re.split(r"[\-/\.]", s)
            if len(parts) >= 3:
                d1 = int(parts[0]) if parts[0].isdigit() else 0
                d2 = int(parts[1]) if parts[1].isdigit() else 0
                if d1 <= 12 and d2 <= 12:
                    ambiguous += 1
                if d1 > 12:
                    day_gt_12 += 1
    return (ambiguous > 0) and (day_gt_12 / max(1, ambiguous) > 0.2)

def to_date_str(x, dayfirst: bool = False) -> str:
    """Parse a date-like string to canonical YYYY-MM-DD or return empty string on failure."""
    if pd.isna(x) or str(x).strip() == "":
        return ""
    s = str(x).strip()
    try:
        dt = dateparser.parse(s, dayfirst=dayfirst, yearfirst=False, fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

# -----------------------
# Numeric parsing (locale-aware)
# -----------------------
def _detect_decimal_and_thousands(sample: str) -> Tuple[str, Optional[str]]:
    """Return best-guess decimal separator and thousands separator for a numeric-looking string.
    
    Examples:
      "1.234,56" -> (",", ".")
      "1,234.56" -> (".", ",")
      "1234,56"  -> (",", None)
      "1234.56"  -> (".", None)
      "1 234,56" -> (",", " ")
    """
    if re.match(r"^-?\d{1,3}(\.\d{3})+,\d+$", sample):
        return (",", ".")
    if re.match(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$", sample):
        return (".", ",")
    if re.match(r"^-?\d+,\d+$", sample):
        return (",", None)
    if re.match(r"^-?\d+\.\d+$", sample):
        return (".", None)
    if re.match(r"^-?\d{1,3}( \d{3})+(,\d+)?$", sample):
        return (",", " ")
    return (".", None)

def to_numeric_series(series: pd.Series) -> pd.Series:
    """Convert a mixed-format numeric column to float using per-value locale detection.
    
    - Removes thousands separators (., or space) as detected.
    - Converts decimal comma to dot when needed.
    - Returns NaN for values that can't be parsed.
    """
    def conv(v):
        if pd.isna(v):
            return np.nan
        s = str(v).strip()
        if s == "":
            return np.nan
        dec, thou = _detect_decimal_and_thousands(s)
        if thou:
            s = s.replace(thou, "")
        if dec != ".":
            s = s.replace(dec, ".")  # unify decimal to '.'
        s = s.replace(" ", "")       # drop stray spaces
        try:
            return float(s)
        except Exception:
            return np.nan
    return series.apply(conv)

# -----------------------
# Delimiter detection & CSV reader
# -----------------------
def _detect_delimiter(path: Path) -> Optional[str]:
    """Detect whether semicolons dominate over commas in the file header.
    
    Returns ';' if semicolons look more prevalent; otherwise None (let pandas sniff).
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            head = "\n".join([next(f) for _ in range(5)])
    except Exception:
        return None
    sc = head.count(";")
    cc = head.count(",")
    if sc > cc:
        return ";"
    return None  # let pandas choose

def read_csv_smart(path: Path) -> pd.DataFrame:
    """Read CSV robustly as strings first; detect semicolons; normalize column names.
    
    We keep columns as strings to preserve raw forms; callers are responsible
    for type coercion via normalize_dataframe().
    """
    sep = _detect_delimiter(Path(path))
    if sep is None:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=["", "NA", "N/A", "null", "None"], engine="python", sep=None)
    else:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=["", "NA", "N/A", "null", "None"], sep=sep)
    df.columns = normalize_colnames(df.columns)
    return df

# -----------------------
# Field typing helpers
# -----------------------
def is_date_col(name: str) -> bool:
    n = name.lower()
    return any(h in n for h in DATE_HINTS)

def is_money_col(name: str) -> bool:
    n = name.lower()
    return any(h in n for h in MONEY_HINTS)

def is_share_col(name: str) -> bool:
    n = name.lower()
    return any(h in n for h in SHARE_HINTS)

def is_rate_col(name: str) -> bool:
    n = name.lower()
    return any(h in n for h in RATE_HINTS)

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with date & numeric columns coerced and canonical key columns present.
    
    - Maps synonyms so COAC_EVENT_KEY and BANK_ACCOUNTS exist if possible.
    - Converts date-like columns to YYYY-MM-DD using per-column day-first inference.
    - Converts numeric-like columns to floats via locale-aware parsing.
    - Upper-cases currency-like columns (any header containing 'currency'/'currencies').
    """
    out = df.copy()

    # Ensure keys exist (if synonyms present). We *don't* invent missing keys.
    cols_lower = {c.lower(): c for c in out.columns}
    if KEY_COAC not in out.columns:
        for k, v in cols_lower.items():
            if SYNONYMS.get(k) == KEY_COAC:
                out[KEY_COAC] = out.pop(v)
                break
    if KEY_BANK not in out.columns:
        for k, v in cols_lower.items():
            if SYNONYMS.get(k) == KEY_BANK:
                out[KEY_BANK] = out.pop(v)
                break

    # Dates
    for c in list(out.columns):
        if is_date_col(c):
            dayfirst = _infer_dayfirst(out[c])
            out[c] = out[c].apply(lambda x: to_date_str(x, dayfirst=dayfirst))

    # Numerics (money/shares/rates)
    for c in list(out.columns):
        if is_money_col(c) or is_share_col(c) or is_rate_col(c):
            out[c] = to_numeric_series(out[c])

    # Currency casing normalization for any currency-like column
    for c in list(out.columns):
        if "currency" in c.lower() or "currencies" in c.lower():
            out[c] = out[c].astype(str).str.strip().str.upper()

    return out
