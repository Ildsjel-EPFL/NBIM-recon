"""
Strict Breaks Reconciliation
----------------------------
Compares Custody vs NBIM dividend data using an explicit column dictionary.

Definition of a break:
- "mismatch": a (COAC_EVENT_KEY, BANK_ACCOUNTS) pair exists in BOTH files, and at least one of the
  mapped columns (excluding the keys) differs after sensible normalization (dates, numbers, currency case).
- "missing at Custody": a key pair appears only in NBIM.
- "missing at NBIM": a key pair appears only in Custody.

Output: a CSV with columns:
- COAC_EVENT_KEY
- BANK_ACCOUNTS
- BREAK_TYPE  (mismatch | missing at Custody | missing at NBIM)
- COLUMN       (for mismatches: the business column name; for missing rows: '')
- CUSTODY_VALUE
- NBIM_VALUE

Usage (from the folder with your CSVs):
    python strict_breaks_reconciliation.py \
        --custody "CUSTODY_Dividend_Bookings 1 (2).csv" \
        --nbim "NBIM_Dividend_Bookings 1 (2).csv" \
        --out "breaks_flags.csv"

Notes:
- The script is resilient to separators and encodings.
- It standardizes headers to UPPERCASE to match the dictionary exactly.
- Numeric/date/currency normalization reduces false positives while remaining strict.
"""

# ---------------------------
# 1) Imports
# ---------------------------
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Set


# ---------------------------
# 2) Utilities
# ---------------------------
def robust_read_csv(path: Path) -> pd.DataFrame:
    trials: List[Tuple[str, str]] = []
    for sep in [",", ";", "|", "\t"]:
        for enc in ["utf-8-sig", "utf-8", "cp1252", "latin1"]:
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc)
                # Heuristic: if single column with many separators inside, keep trying
                if df.shape[1] == 1 and df.iloc[:5, 0].astype(str).str.contains(sep).any():
                    trials.append((sep, enc))
                    continue
                return df
            except Exception:
                trials.append((sep, enc))
    raise RuntimeError(f"Unable to read {path}. Tried separators/encodings like: {trials[:4]} ...")

def standardize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df

def ensure_columns(df: pd.DataFrame, cols: List[str], origin: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{origin} missing required columns: {missing}")

def to_date_str(v: Any) -> str:
    if pd.isna(v): return ""
    # Try parse with pandas
    try:
        dt = pd.to_datetime(v, errors="coerce", dayfirst=False)
        if pd.isna(dt): return str(v).strip()
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return str(v).strip()

def to_numeric(v: Any) -> float:
    if pd.isna(v) or v == "": return np.nan
    # remove common thousands/space
    s = str(v).replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return np.nan

def equal_numeric(a: Any, b: Any, tol: float) -> bool:
    fa, fb = to_numeric(a), to_numeric(b)
    if np.isnan(fa) and np.isnan(fb): return True
    if np.isnan(fa) or np.isnan(fb): return False
    return abs(fa - fb) <= tol

def equal_date(a: Any, b: Any) -> bool:
    return to_date_str(a) == to_date_str(b)

def equal_currency(a: Any, b: Any) -> bool:
    sa = "" if pd.isna(a) else str(a).strip().upper()
    sb = "" if pd.isna(b) else str(b).strip().upper()
    return sa == sb

def equal_text(a: Any, b: Any) -> bool:
    sa = "" if pd.isna(a) else str(a).strip()
    sb = "" if pd.isna(b) else str(b).strip()
    return sa == sb

def values_equal(field: str, custody_val: Any, nbim_val: Any, DATE_FIELDS : Set[str], CURRENCY_FIELDS : Set[str], NUMERIC_FIELDS : Set[str], NUM_TOL : float) -> bool:
    if field in DATE_FIELDS:
        return equal_date(custody_val, nbim_val)
    if field in CURRENCY_FIELDS:
        return equal_currency(custody_val, nbim_val)
    if field in NUMERIC_FIELDS:
        return equal_numeric(custody_val, nbim_val, tol=NUM_TOL)
    # default textual compare
    return equal_text(custody_val, nbim_val)

# ---------------------------
# 3) Core logic
# ---------------------------
def reconcile_breaks(custody_csv: Path, nbim_csv: Path, out_csv: Path = "breaks_flag.csv") -> str:
    COLUMN_DICT: Dict[str, List[str]] = {
        "COAC_EVENT_KEY" : ["COAC_EVENT_KEY", "COAC_EVENT_KEY"],
        "BANK_ACCOUNTS"  : ["BANK_ACCOUNTS", "BANK_ACCOUNT"],
        "ISIN"           : ["ISIN", "ISIN"],
        "SEDOL"          : ["SEDOL", "SEDOL"],
        "NOMINAL_BASIS"  : ["NOMINAL_BASIS", "NOMINAL_BASIS"],
        "EX_DATE"        : ["EX_DATE", "EXDATE"],
        "PAY_DATE"       : ["PAY_DATE", "PAYMENT_DATE"],
        "CURRENCIES"     : ["CURRENCIES", "QUOTATION_CURRENCY"],
        "DIV_RATE"       : ["DIV_RATE", "DIVIDENDS_PER_SHARE"],
        "TAX_RATE"       : ["TAX_RATE", "WTHTAX_RATE"],
        "GROSS_AMOUNT"   : ["GROSS_AMOUNT", "GROSS_AMOUNT_QUOTATION"],
        "NET_AMOUNT_QC"  : ["NET_AMOUNT_QC", "NET_AMOUNT_QUOTATION"],
        "TAX"            : ["TAX", "WTHTAX_COST_QUOTATION"],
        "NET_AMOUNT_SC"  : ["NET_AMOUNT_SC", "NET_AMOUNT_SETTLEMENT"],
        "SETTLED_CURRENCY": ["SETTLED_CURRENCY", "SETTLEMENT_CURRENCY"],
    }

    # Business types for smarter normalization (still strict)
    DATE_FIELDS = {"EX_DATE", "PAY_DATE"}
    CURRENCY_FIELDS = {"CURRENCIES", "SETTLED_CURRENCY"}
    NUMERIC_FIELDS = {
        "DIV_RATE", "TAX_RATE", "GROSS_AMOUNT", "NET_AMOUNT_QC", "TAX", "NET_AMOUNT_SC",
        "NOMINAL_BASIS"  # often numeric, include for safety
    }
    # Tolerance for floats (kept very small so it's effectively strict but avoids 0.30000004)
    NUM_TOL = 1e-9

    # Read
    df_c = robust_read_csv(custody_csv)
    df_n = robust_read_csv(nbim_csv)

    # Uppercase headers to match dictionary exactly
    df_c = standardize_headers(df_c)
    df_n = standardize_headers(df_n)

    # Validate presence of mapped columns
    custody_required = [v[0] for v in COLUMN_DICT.values()]
    nbim_required   = [v[1] for v in COLUMN_DICT.values()]
    ensure_columns(df_c, custody_required, "Custody")
    ensure_columns(df_n, nbim_required,   "NBIM")

    # Select only columns we will use (keeps runtime clean)
    df_c_sel = df_c[[v[0] for v in COLUMN_DICT.values()]].copy()
    df_n_sel = df_n[[v[1] for v in COLUMN_DICT.values()]].copy()

    # Rename to a common schema (business names)
    rename_c = {v[0]: k for k, v in COLUMN_DICT.items()}
    rename_n = {v[1]: k for k, v in COLUMN_DICT.items()}
    c_std = df_c_sel.rename(columns=rename_c)
    n_std = df_n_sel.rename(columns=rename_n)

    # Ensure keys present
    for k in ["COAC_EVENT_KEY", "BANK_ACCOUNTS"]:
        if k not in c_std.columns or k not in n_std.columns:
            raise KeyError(f"Required key '{k}' missing after normalization.")

    # Some datasets might have duplicates on key-pair. We'll keep the first to allow strict, row-level comparison.
    c_std = c_std.drop_duplicates(subset=["COAC_EVENT_KEY", "BANK_ACCOUNTS"], keep="first")
    n_std = n_std.drop_duplicates(subset=["COAC_EVENT_KEY", "BANK_ACCOUNTS"], keep="first")

    # Outer-join on the exact business keys
    merged = c_std.merge(
        n_std,
        on=["COAC_EVENT_KEY", "BANK_ACCOUNTS"],
        how="outer",
        suffixes=("_C", "_N"),
        indicator=True
    )

    # Prepare output rows
    out_rows: List[Dict[str, Any]] = []

    # 3a) Missing on one side
    # _merge values: 'left_only' => only in custody; 'right_only' => only in NBIM; 'both' => matched
    missing_cust = merged[merged["_merge"] == "right_only"]
    for _, r in missing_cust.iterrows():
        out_rows.append({
            "COAC_EVENT_KEY": r["COAC_EVENT_KEY"],
            "BANK_ACCOUNTS":  r["BANK_ACCOUNTS"],
            "BREAK_TYPE":     "missing at Custody",
            "COLUMN":         "",
            "CUSTODY_VALUE":  "",
            "NBIM_VALUE":     "",
        })

    missing_nbim = merged[merged["_merge"] == "left_only"]
    for _, r in missing_nbim.iterrows():
        out_rows.append({
            "COAC_EVENT_KEY": r["COAC_EVENT_KEY"],
            "BANK_ACCOUNTS":  r["BANK_ACCOUNTS"],
            "BREAK_TYPE":     "missing at NBIM",
            "COLUMN":         "",
            "CUSTODY_VALUE":  "",
            "NBIM_VALUE":     "",
        })

    # 3b) Mismatches for pairs present in both
    both = merged[merged["_merge"] == "both"].copy()
    compare_fields = [k for k in COLUMN_DICT.keys() if k not in ("COAC_EVENT_KEY", "BANK_ACCOUNTS")]

    for _, row in both.iterrows():
        for field in compare_fields:
            c_val = row.get(f"{field}_C")
            n_val = row.get(f"{field}_N")
            if not values_equal(field, c_val, n_val, DATE_FIELDS, CURRENCY_FIELDS, NUMERIC_FIELDS, NUM_TOL):
                out_rows.append({
                    "COAC_EVENT_KEY": row["COAC_EVENT_KEY"],
                    "BANK_ACCOUNTS":  row["BANK_ACCOUNTS"],
                    "BREAK_TYPE":     "mismatch",
                    "COLUMN":         field,
                    "CUSTODY_VALUE":  c_val,
                    "NBIM_VALUE":     n_val,
                })

    # Build output DataFrame (LLM-friendly long format: one break per row)
    out_df = pd.DataFrame(out_rows, columns=[
        "COAC_EVENT_KEY", "BANK_ACCOUNTS", "BREAK_TYPE", "COLUMN", "CUSTODY_VALUE", "NBIM_VALUE"
    ])

    # Save
    out_df.to_csv(out_csv, index=False)
    print(f"Done. Breaks saved to: {out_csv}")
    print(f"Total breaks: {len(out_df)}")
    return out_csv
