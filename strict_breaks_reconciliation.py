"""### strict_breaks_reconciliation.py
Strict (deterministic) reconciliation using explicit column mapping.

Responsibilities:
- Read semicolon- or comma-delimited CSVs; normalize dates, numbers and currencies.
- Join datasets on (COAC_EVENT_KEY, BANK_ACCOUNTS/ACCOUNT) with robust aliasing.
- Compare EXACTLY the column pairs provided by the business (see COMPARE_MAP).
- Produce 'breaks_flags.csv' with: status, mismatch reasons, and which pairs failed.

Key design choices:
- We avoid comparing "custodian name" type fields (handled upstream in utils_io for general comparisons).
- Explicit mapping is used instead of "column intersection" so we never miss a requested field pair.
- Aliases are symmetric where appropriate so variations (e.g., EX_DATE vs EXDATE) are recognized.
- Type-aware comparison: dates exact post-normalization; currencies case-insensitive;
  money with ±0.01 tolerance; rates with ±1e-4.
-----------------------------------------------------------------------------"""

from __future__ import annotations
from pathlib import Path
import pandas as pd

from utils_io import (
    KEY_COAC, KEY_BANK,
    read_csv_smart, normalize_dataframe
)

# ---------------------
# Tolerances
# ---------------------
MONEY_TOL = 0.01  # in quotation/settlement currency; aligns to the playbook
SHARE_TOL = 1e-6  # kept for completeness when comparing share-like fields
RATE_TOL  = 1e-4  # for rates/percentages

# ---------------------
# Explicit business mapping (Custody, NBIM, type)
# ---------------------
# 'type' controls comparison logic: one of {'text','date','currency','rate','money'}
COMPARE_MAP = [
    ("COAC_EVENT_KEY", "COAC_EVENT_KEY", "text"),
    ("BANK_ACCOUNTS",  "BANK_ACCOUNT",  "text"),
    ("ISIN",           "ISIN",          "text"),
    ("SEDOL",          "SEDOL",         "text"),
    ("NOMINAL_BASIS",  "NOMINAL_BASIS", "text"),
    ("EX_DATE",        "EXDATE",        "date"),
    ("PAY_DATE",       "PAYMENT_DATE",  "date"),
    ("CURRENCIES",     "QUOTATION_CURRENCY", "currency"),
    ("DIV_RATE",       "DIVIDENDS_PER_SHARE", "rate"),
    ("TAX_RATE",       "WTHTAX_RATE",   "rate"),
    ("GROSS_AMOUNT",   "GROSS_AMOUNT_QUOTATION", "money"),
    ("NET_AMOUNT_QC",  "NET_AMOUNT_QUOTATION",   "money"),
    ("TAX",            "WTHTAX_COST_QUOTATION",  "money"),
    ("NET_AMOUNT_SC",  "NET_AMOUNT_SETTLEMENT",  "money"),
    ("SETTLED_CURRENCY","SETTLEMENT_CURRENCY",   "currency"),
]

# ---------------------
# Header aliases (symmetric where useful)
# ---------------------
ALIASES = {
    # Keys
    "BANK_ACCOUNTS": ["BANK_ACCOUNT","BANK_ACCT","ACCOUNT","ACCT"],
    "BANK_ACCOUNT":  ["BANK_ACCOUNTS","BANK_ACCT","ACCOUNT","ACCT"],
    "COAC_EVENT_KEY":["COAC KEY","EVENT_KEY","EVENT ID","COACKEY","COAC-EVENT-KEY"],

    # Dates & currencies
    "EX_DATE": ["EXDATE","EX-DATE","EX DATE"],
    "EXDATE":  ["EX_DATE","EX-DATE","EX DATE"],
    "PAY_DATE": ["PAYMENT_DATE","PAYDATE","PAY DATE"],
    "PAYMENT_DATE": ["PAY_DATE","PAYDATE","PAY DATE"],
    "CURRENCIES": ["QUOTATION_CURRENCY","CURRENCY","QUOTATIONCURRENCY"],
    "QUOTATION_CURRENCY": ["CURRENCIES","QUOTATIONCURRENCY","CCY_QUOTE"],

    # Amounts & rates
    "DIV_RATE": ["DIVIDENDS_PER_SHARE","DIVIDEND_PER_SHARE","DIV_PER_SHARE","DIV_PER_SHR","DIVIDENDSPS","DIVPS"],
    "DIVIDENDS_PER_SHARE": ["DIV_RATE","DIV_PER_SHARE","DIV_PER_SHR","DIVPS"],
    "TAX_RATE": ["WTHTAX_RATE","WITHHOLDING_TAX_RATE"],
    "WTHTAX_RATE": ["TAX_RATE","WITHHOLDING_TAX_RATE"],
    "GROSS_AMOUNT": ["GROSS_AMOUNT_QUOTATION","GROSS_AMOUNT_QC","GROSS_QC"],
    "GROSS_AMOUNT_QUOTATION": ["GROSS_AMOUNT","GROSS_AMOUNT_QC","GROSS_QC"],
    "NET_AMOUNT_QC": ["NET_AMOUNT_QUOTATION","NET_QC"],
    "NET_AMOUNT_QUOTATION": ["NET_AMOUNT_QC","NET_QC"],
    "TAX": ["WTHTAX_COST_QUOTATION","WTHTAX_QUOTATION","TAX_COST_QC"],
    "WTHTAX_COST_QUOTATION": ["TAX","WTHTAX_QUOTATION","TAX_COST_QC"],
    "NET_AMOUNT_SC": ["NET_AMOUNT_SETTLEMENT","NET_SC","NET_SETTLEMENT"],
    "NET_AMOUNT_SETTLEMENT": ["NET_AMOUNT_SC","NET_SC","NET_SETTLEMENT"],
    "SETTLED_CURRENCY": ["SETTLEMENT_CURRENCY","SETTLED_CCY","SETTLEMENT_CCY"],
    "SETTLEMENT_CURRENCY": ["SETTLED_CURRENCY","SETTLED_CCY","SETTLEMENT_CCY"],
}

def _canon(s: str) -> str:
    """Canonicalize a column label by uppercasing and stripping non-alphanumerics.
    This helps match headers like 'EX-DATE' vs 'EX_DATE' vs 'exdate'."""
    return "".join(ch for ch in s.upper() if ch.isalnum())

def _find_col(df: pd.DataFrame, desired: str) -> str | None:
    """Find the best-matching column in df for the desired header using several passes:
    1) Exact match
    2) Case-insensitive exact
    3) Alias-based (symmetric) search
    4) Canonical form (uppercase alnum only) search
    Returns the actual df column name or None if not found.
    """
    # 1) exact
    if desired in df.columns:
        return desired
    # 2) case-insensitive exact
    lower_map = {c.lower(): c for c in df.columns}
    if desired.lower() in lower_map:
        return lower_map[desired.lower()]
    # 3) alias-based (symmetric)
    cands = [desired] + ALIASES.get(desired.upper(), [])
    for cand in cands:
        if cand in df.columns:
            return cand
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    # 4) canonical form (remove non-alnum)
    canon_map = {_canon(c): c for c in df.columns}
    for cand in cands:
        cc = _canon(cand)
        if cc in canon_map:
            return canon_map[cc]
    return None

def _values_equal_by_type(v1, v2, kind: str) -> bool:
    """Type-aware equality with tolerances where relevant."""
    if kind == "money":
        try:
            f1 = float(v1) if v1 == v1 else float("nan")
            f2 = float(v2) if v2 == v2 else float("nan")
        except Exception:
            return False
        if pd.isna(f1) and pd.isna(f2): return True
        if pd.isna(f1) or pd.isna(f2):  return False
        return abs(f1 - f2) <= MONEY_TOL
    if kind == "rate":
        try:
            f1 = float(v1) if v1 == v1 else float("nan")
            f2 = float(v2) if v2 == v2 else float("nan")
        except Exception:
            return False
        if pd.isna(f1) and pd.isna(f2): return True
        if pd.isna(f1) or pd.isna(f2):  return False
        return abs(f1 - f2) <= RATE_TOL
    if kind == "date":
        s1 = "" if pd.isna(v1) else str(v1)
        s2 = "" if pd.isna(v2) else str(v2)
        # Dates are normalized upstream to YYYY-MM-DD
        return s1 == s2
    if kind == "currency":
        s1 = "" if pd.isna(v1) else str(v1).strip().upper()
        s2 = "" if pd.isna(v2) else str(v2).strip().upper()
        return s1 == s2
    # text default (trimmed string comparison)
    s1 = "" if pd.isna(v1) else str(v1).strip()
    s2 = "" if pd.isna(v2) else str(v2).strip()
    return s1 == s2

def reconcile_breaks(custody_csv: Path, nbim_csv: Path, out_csv: Path = Path("breaks_flags.csv")) -> Path:
    """Run the strict reconciliation and persist a 'breaks_flags.csv' file.
    
    Steps:
    1) Read both files (semicolon-aware), normalize dates/numbers/currencies.
    2) Resolve the join keys with robust aliasing:
       - Custody:  COAC_EVENT_KEY + BANK_ACCOUNTS (or BANK_ACCOUNT)
       - NBIM:     COAC_EVENT_KEY + BANK_ACCOUNT (or BANK_ACCOUNTS)
    3) Outer-join on the resolved keys to detect missing keys on either side.
    4) For rows present on both sides, compare the explicit pairs in COMPARE_MAP.
    5) Write a tidy CSV with one row per break or missing key.
    """
    # 1) Read and normalize
    custody_raw = read_csv_smart(Path(custody_csv))
    nbim_raw    = read_csv_smart(Path(nbim_csv))
    custody = normalize_dataframe(custody_raw)
    nbim    = normalize_dataframe(nbim_raw)

    # 2) Resolve join keys with aliases
    cust_key1 = _find_col(custody, "COAC_EVENT_KEY") or "COAC_EVENT_KEY"
    cust_key2 = _find_col(custody, "BANK_ACCOUNTS") or _find_col(custody, "BANK_ACCOUNT") or "BANK_ACCOUNTS"
    nbim_key1 = _find_col(nbim, "COAC_EVENT_KEY") or "COAC_EVENT_KEY"
    nbim_key2 = _find_col(nbim, "BANK_ACCOUNT")   or _find_col(nbim, "BANK_ACCOUNTS") or "BANK_ACCOUNT"

    # Guardrails: ensure keys exist in each df before joining
    for dfname, df, k1, k2 in [
        ("Custody", custody, cust_key1, cust_key2),
        ("NBIM", nbim, nbim_key1, nbim_key2),
    ]:
        for key in (k1, k2):
            if key not in df.columns:
                raise ValueError(f"{dfname} file missing required key column '{key}'. Got columns: {list(df.columns)}")

    # 3) Create normalized join columns and outer-join on keys
    csmall = custody.copy()
    nsmall = nbim.copy()
    csmall[KEY_COAC] = csmall[cust_key1]
    csmall[KEY_BANK] = csmall[cust_key2]
    nsmall[KEY_COAC] = nsmall[nbim_key1]
    nsmall[KEY_BANK] = nsmall[nbim_key2]

    merged = csmall[[KEY_COAC, KEY_BANK]].merge(
        nsmall[[KEY_COAC, KEY_BANK]], on=[KEY_COAC, KEY_BANK], how="outer", indicator=True
    )

    rows = []

    # 4) Missing keys
    left_only = merged[merged["_merge"] == "left_only"]
    for _, r in left_only.iterrows():
        rows.append({KEY_COAC: r[KEY_COAC], KEY_BANK: r[KEY_BANK], "status": "missing at NBIM", "reason": "Key present in Custody only."})
    right_only = merged[merged["_merge"] == "right_only"]
    for _, r in right_only.iterrows():
        rows.append({KEY_COAC: r[KEY_COAC], KEY_BANK: r[KEY_BANK], "status": "missing at Custody", "reason": "Key present in NBIM only."})

    # 5) Key pairs present on both sides: compare the explicit pairs
    both = merged[merged["_merge"] == "both"][[KEY_COAC, KEY_BANK]]
    if not both.empty:
        # Index for efficient row lookups
        cidx = csmall.set_index([KEY_COAC, KEY_BANK])
        nidx = nsmall.set_index([KEY_COAC, KEY_BANK])
        for key_vals in both.itertuples(index=False):
            k1, k2 = key_vals
            try:
                crow = cidx.loc[(k1, k2)]
                nrow = nidx.loc[(k1, k2)]
            except KeyError:
                # If either side is missing unexpectedly, skip (already reported)
                continue

            mismatches = []
            reasons = []
            for left_name, right_name, kind in COMPARE_MAP:
                if left_name in ("COAC_EVENT_KEY","BANK_ACCOUNTS"):  # skip the key columns; already matched
                    continue
                # Resolve the actual df columns on each side using the alias machinery
                lc = _find_col(csmall, left_name)
                rc = _find_col(nsmall, right_name)
                if lc is None or rc is None:
                    # Report missing columns as mismatches for visibility
                    miss = left_name if lc is None else right_name
                    mismatches.append(f"{left_name}~{right_name}")
                    reasons.append(f"{left_name} vs {right_name}: missing column '{miss}'")
                    continue

                v1 = crow[lc] if lc in crow.index else None
                v2 = nrow[rc] if rc in nrow.index else None
                if not _values_equal_by_type(v1, v2, kind):
                    mismatches.append(f"{left_name}~{right_name}")
                    reasons.append(f"{left_name}={v1} vs {right_name}={v2}")

            if mismatches:
                rows.append({
                    KEY_COAC: k1,
                    KEY_BANK: k2,
                    "status": "mismatch",
                    "reason": "; ".join(reasons)[:2000],
                    "mismatch_columns": ",".join(mismatches)
                })

    # Emit the tidy CSV
    out_df = pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)
    out_df.to_csv(out_csv, index=False)
    return Path(out_csv)
