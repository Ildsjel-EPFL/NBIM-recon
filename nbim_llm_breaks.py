"""
NBIM LLM Breaks Analysis
------------------
Analyze reconciliation breaks with an LLM (OpenAI via LangChain), now with
full-row context from the original Custody and NBIM datasets.

Requirements:
    pip install pandas langchain-core langchain-openai openai transformers torch
    #### Set your key in a .env file: OPENAI_API_KEY="sk-XXXX"
"""
# ---------------------------
# 1) Imports
# ---------------------------
import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

# ---------------------------
# 2) Utilities
# ---------------------------

# ---- Hugging Face tokenizer to control prompt size ----
try:
    from transformers import AutoTokenizer
    _tokenizer = AutoTokenizer.from_pretrained("gpt2")
except Exception:
    _tokenizer = None

# ---- LangChain + OpenAI ----
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI


# ---------- Domain column map (also used to pick context columns) ----------
# This mirrors the comparison dictionary you provided, so we can ship only
# the relevant business columns to the LLM instead of the entire raw row.
COLUMN_DICT: Dict[str, List[str]] = {
    "COAC_EVENT_KEY" : ["COAC_EVENT_KEY", "COAC_EVENT_KEY"],
    "BANK_ACCOUNTS"  : ["BANK_ACCOUNTS", "BANK_ACCOUNTS"],
    "ISIN"           : ["ISIN", "ISIN"],
    "SEDOL"          : ["SEDOL", "SEDOL"],
    "CUSTODIAN"      : ["CUSTODIAN", "CUSTODIAN"],
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

CONTEXT_COLS_CUSTODY = [v[0] for v in COLUMN_DICT.values()]
CONTEXT_COLS_NBIM    = [v[1] for v in COLUMN_DICT.values()]

# ---------- Prompts ----------
SYSTEM_PROMPT = """You are a senior operations analyst for equity dividends.
You will receive:
1) The list of break rows for ONE transaction (same COAC_EVENT_KEY and BANK_ACCOUNTS).
2) The Custody record for that transaction (selected columns).
3) The NBIM record for that transaction (selected columns).

Your task:
- Diagnose precisely WHY this transaction is flagged (point-by-point).
- Classify the issue.
- Assign an operational severity.
- Propose concrete remediation steps.
- Draft a short email to the custodian if relevant.

Return STRICT JSON with keys:
- reason: a point-by-point, bullet-style description where EACH line begins with "- " (dash+space).
- category: one of ["Missing at Custody","Missing at NBIM","Amount mismatch","Currency mismatch","Date mismatch","Identifier mismatch","Other mismatch"]
- severity: one of ["High","Medium","Low"]
- remediation_steps: 2-5 bullet points in ONE string where each item begins with "- "
- email_to_custodian: 3-6 polite sentences; empty string "" if not needed
"""

USER_TEMPLATE = """Transaction:
- COAC_EVENT_KEY: {event_key}
- BANK_ACCOUNTS: {bank_account}

Break rows:
{break_rows}

Custody record (selected columns):
{custody_record}

NBIM record (selected columns):
{nbim_record}

Playbook:
{playbook}

Return JSON only.
"""

# ---------- Helpers ----------
def _token_count(text: str) -> int:
    if _tokenizer is None:
        return max(1, len(text) // 4)
    try:
        return len(_tokenizer.encode(text))
    except Exception:
        return max(1, len(text) // 4)

def _limit_text(text: str, max_tokens: int) -> str:
    if _token_count(text) <= max_tokens:
        return text
    # Keep start and end context if too long
    head = text[:4000]
    tail = text[-2000:]
    return head + "\n... [truncated] ...\n" + tail

def _standardize_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().upper() for c in out.columns]
    return out

def _robust_read_csv(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if path is None:
        return None
    if not Path(path).exists():
        return None
    for sep in [",", ";", "|", "\t"]:
        for enc in ["utf-8-sig", "utf-8", "cp1252", "latin1"]:
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc)
                if df.shape[1] == 1 and df.iloc[:5,0].astype(str).str.contains(sep).any():
                    continue
                return df
            except Exception:
                continue
    return None

def _pick_context_row(df: Optional[pd.DataFrame],
                      key_event: Any,
                      key_acct: Any,
                      wanted_cols: List[str]) -> Dict[str, Any]:
    """
    Returns a dict with selected columns for the (COAC_EVENT_KEY, BANK_ACCOUNTS) row.
    If multiple rows exist, we pick the first occurrence to keep the prompt concise.
    """
    if df is None:
        return {"_notice": "source file not provided"}
    sdf = _standardize_headers(df)
    # missing columns are OK — we only keep what exists
    cols = [c for c in wanted_cols if c in sdf.columns]
    # Require key columns to select the row
    if "COAC_EVENT_KEY" not in sdf.columns or "BANK_ACCOUNTS" not in sdf.columns:
        return {"_notice": "source file missing key columns"}
    hit = sdf[(sdf["COAC_EVENT_KEY"] == key_event) & (sdf["BANK_ACCOUNTS"] == key_acct)]
    if hit.empty:
        return {"_notice": "no matching row for provided keys"}
    row = hit.iloc[0]
    return {c: row.get(c, None) for c in cols}

def _render_break_rows(group_df: pd.DataFrame) -> str:
    lines = []
    for _, r in group_df.iterrows():
        lines.append(
            f"- BREAK_TYPE={r.get('BREAK_TYPE','')}; "
            f"COLUMN={r.get('COLUMN','')}; "
            f"CUSTODY_VALUE={r.get('CUSTODY_VALUE','')}; "
            f"NBIM_VALUE={r.get('NBIM_VALUE','')}"
        )
    text = "\n".join(lines)
    return _limit_text(text, max_tokens=2200)

def _json_compact(d: Dict[str, Any]) -> str:
    try:
        return json.dumps(d, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        # Last resort
        return str(d)

def _load_playbook() -> str:
    for name in ["llm_playbook.txt", "PLAYBOOK.md", "playbook.txt"]:
        p = Path(name)
        if p.exists():
            try:
                txt = p.read_text(encoding="utf-8")
                return _limit_text(txt, max_tokens=1500)
            except Exception:
                continue
    return "(no internal SOP provided)"

# ---------- Public API ----------
def run_llm_break_analysis(
    breaks_flags_path: str,
    custody_csv: Optional[str] = None,
    nbim_csv: Optional[str] = None,
    out_csv: str = "breaks_analysis.csv",
    model: str = "gpt-4o-mini",
    temperature: float = 0.0
) -> pd.DataFrame:
    """
    Run the entire LLM analysis with optional full-row context.

    Parameters
    ----------
    breaks_flags_path : str
        Path to breaks_flags.csv (one break per row)
    custody_csv : Optional[str]
        Path to the original Custody CSV. If provided, the LLM gets the matched row.
    nbim_csv : Optional[str]
        Path to the original NBIM CSV. If provided, the LLM gets the matched row.
    out_csv : str
        Output filename for the analysis (one row per transaction)
    model : str
        OpenAI chat model (e.g., "gpt-4o-mini", "gpt-4.1-mini", etc.)
    temperature : float
        OpenAI temperature parameter (0.0 = most deterministic)

    Returns
    -------
    pd.DataFrame
        The analysis table that was written to `out_csv`.
    """
    if not os.getenv("OPENAI_API_KEY"):
        raise EnvironmentError("Missing OPENAI_API_KEY environment variable.")

    breaks_path = Path(breaks_flags_path)
    if not breaks_path.exists():
        raise FileNotFoundError(f"breaks_flags file not found: {breaks_flags_path}")

    # Load breaks
    df = pd.read_csv(breaks_path)
    required = ["COAC_EVENT_KEY", "BANK_ACCOUNTS", "BREAK_TYPE", "COLUMN", "CUSTODY_VALUE", "NBIM_VALUE"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"breaks_flags file missing columns: {missing}")

    # Load optional full datasets for context
    df_custody = _robust_read_csv(Path(custody_csv)) if custody_csv else None
    df_nbim    = _robust_read_csv(Path(nbim_csv)) if nbim_csv else None

    # Prepare LLM
    llm = ChatOpenAI(model=model, temperature=temperature)
    prompt = ChatPromptTemplate.from_messages([("system", SYSTEM_PROMPT), ("user", USER_TEMPLATE)])
    playbook = _load_playbook()

    # Group by transaction (unique key)
    grouped = df.groupby(["COAC_EVENT_KEY", "BANK_ACCOUNTS"], dropna=False, sort=False)

    outputs = []
    for (event_key, bank_acct), group in grouped:
        # Build concise context
        breaks_text = _render_break_rows(group)
        custody_row = _pick_context_row(df_custody, event_key, bank_acct, CONTEXT_COLS_CUSTODY)
        nbim_row    = _pick_context_row(df_nbim,    event_key, bank_acct, CONTEXT_COLS_NBIM)

        custody_json = _json_compact(custody_row)
        nbim_json    = _json_compact(nbim_row)

        # Format prompt
        msgs = prompt.format_messages(
            event_key=str(event_key),
            bank_account=str(bank_acct),
            break_rows=breaks_text,
            custody_record=custody_json,
            nbim_record=nbim_json,
            playbook=playbook
        )

        # Invoke model
        res = llm.invoke(msgs).content

        # Parse STRICT JSON; if parsing fails, keep raw in 'reason'
        reason = ""
        category = "Other mismatch"
        severity = "Medium"
        remediation = ""
        email = ""

        try:
            s = res
            start = s.find("{"); end = s.rfind("}")
            payload = json.loads(s[start:end+1]) if (start != -1 and end != -1 and end > start) else json.loads(s)
            # Enforce the "bullet-by-bullet" in reason: ensure each line begins with "- "
            reason = str(payload.get("reason", "")).strip()
            if reason and not reason.lstrip().startswith("-"):
                # Best-effort normalization if the model returns lines separated by periods
                bullets = [ln.strip() for ln in reason.replace("\r","").split("\n") if ln.strip()]
                bullets = [ln if ln.startswith("- ") else f"- {ln}" for ln in bullets]
                reason = "\n".join(bullets)

            category = str(payload.get("category", category)).strip()
            severity = str(payload.get("severity", severity)).strip()
            remediation = str(payload.get("remediation_steps", remediation)).strip()
            email = str(payload.get("email_to_custodian", email)).strip()
        except Exception:
            # Fallback: keep raw output; convert to bullet list if needed
            raw = res.strip()
            if raw and not raw.lstrip().startswith("-"):
                bullets = [ln.strip() for ln in raw.replace("\r","").split("\n") if ln.strip()]
                bullets = [ln if ln.startswith("- ") else f"- {ln}" for ln in bullets]
                raw = "\n".join(bullets)
            reason = raw

        outputs.append({
            "COAC_EVENT_KEY": event_key,
            "BANK_ACCOUNTS": bank_acct,
            "CATEGORY": category,
            "SEVERITY": severity,
            "REASON": reason,  # <-- bullet-style, point-by-point details per break
            "REMEDIATION_STEPS": remediation,
            "EMAIL_TO_CUSTODIAN": email
        })

    out_df = pd.DataFrame(outputs, columns=[
        "COAC_EVENT_KEY","BANK_ACCOUNTS","CATEGORY","SEVERITY",
        "REASON","REMEDIATION_STEPS","EMAIL_TO_CUSTODIAN"
    ])
    out_df.to_csv(out_csv, index=False)

    print(f"✅ LLM analysis complete — {len(out_df)} transactions")
    print(f"📝 Output saved to: {out_csv}")
    return out_df
