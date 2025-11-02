"""### nbim_llm_breaks.py
LLM-based enrichment of strict reconciliation results.

Responsibilities:
- Accept the strict 'breaks_flags.csv' and the original Custody/NBIM files.
- For each break group (COAC_EVENT_KEY, BANK_ACCOUNTS), construct a prompt
  that includes: the playbook, the strict rows for that group, and the full
  row context from both datasets.
- Use OpenAI JSON mode to obtain a structured object: category, severity,
  explanation, proposed actions, and an email draft to the custodian.
- Enforce a soft USD budget cap per run; skip groups once the cap would be exceeded.

Notes:
- We estimate tokens via tiktoken when available to track the budget.
- We validate/clip the model's JSON to a safe schema before writing output.
-----------------------------------------------------------------------------"""

from __future__ import annotations
import os, json
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

from utils_io import KEY_COAC, KEY_BANK, read_csv_smart, normalize_dataframe

# ------------- Config -------------
# These can be overridden via environment variables or .env
DEFAULT_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
MAX_COST_USD  = float(os.getenv("MAX_COST_USD", "15"))

# Rough cost map (USD per 1k tokens). For accurate accounting, override via env.
INPUT_PER_1K  = float(os.getenv("INPUT_COST_PER_1K", "0.005"))
OUTPUT_PER_1K = float(os.getenv("OUTPUT_COST_PER_1K","0.015"))

# Prefer tiktoken for token estimation if available; otherwise fall back to a simple heuristic.
try:
    import tiktoken
    _enc = tiktoken.get_encoding("cl100k_base")
    def est_tokens(s: str) -> int:
        try:
            return len(_enc.encode(s))
        except Exception:
            return max(1, len(s)//4)
except Exception:
    def est_tokens(s: str) -> int:
        return max(1, len(s)//4)

# ------------- Playbook -------------
def _load_playbook(playbook_path: Optional[Path]) -> str:
    """Load the LLM playbook (SOP). If missing, return a sensible default."""
    if playbook_path and Path(playbook_path).exists():
        return Path(playbook_path).read_text(encoding="utf-8")
    # default short SOP (kept inline to avoid failures if the file is missing)
    return (
        "Dividend reconciliation SOP:\n"
        "- Treat small rounding differences up to 0.01 as matches for monetary amounts.\n"
        "- Dates must match exactly after normalization to YYYY-MM-DD.\n"
        "- SHARES must match within microscopic tolerances; FX rates should be consistent.\n"
        "- For legitimate corporate action reasons (FX rounding, ADR fees, tax rates), propose actions and draft a short email to the custodian when needed."
    )

# Controlled vocab for the model's outputs
ALLOWED_CATEGORIES = {"Rounding", "FX", "Tax", "Data entry error", "Missing booking", "Corporate action nuance", "Unknown"}
ALLOWED_SEVERITY   = {"LOW", "MEDIUM", "HIGH"}

# ------------- OpenAI client -------------
def _ask_llm(messages: List[Dict[str,str]], model: str = DEFAULT_MODEL) -> str:
    """Call OpenAI with JSON mode; return the JSON string response.
    
    We set response_format to {"type": "json_object"} so the model is
    strongly guided to return a single JSON object (no preamble).
    """
    from openai import OpenAI
    client = OpenAI()
    resp = client.chat.completions.create(
        model=model,
        response_format={"type": "json_object"},
        messages=messages,
        temperature=0.2,  # conservative decoding for deterministic, auditable outputs
    )
    return resp.choices[0].message.content or "{}"

def _validate_payload(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the model's JSON: coerce missing/invalid fields to safe defaults.
    
    - Category must be in ALLOWED_CATEGORIES; otherwise 'Unknown'.
    - Severity must be one of LOW/MEDIUM/HIGH; default MEDIUM.
    - Explanation clipped to 2000 chars.
    - Proposed actions must be a list of strings (or coerced to such).
    - Email draft clipped to 2000 chars.
    """
    out = {
        "category": str(obj.get("category","Unknown")),
        "severity": str(obj.get("severity","MEDIUM")).upper(),
        "explanation": str(obj.get("explanation",""))[:2000],
        "proposed_actions": obj.get("proposed_actions", []),
        "custodian_email_draft": str(obj.get("custodian_email_draft",""))[:2000],
    }
    if out["category"] not in ALLOWED_CATEGORIES:
        out["category"] = "Unknown"
    if out["severity"] not in ALLOWED_SEVERITY:
        out["severity"] = "MEDIUM"
    if not isinstance(out["proposed_actions"], list):
        out["proposed_actions"] = [str(out["proposed_actions"])]
    return out

def run_llm_break_analysis(
    breaks_csv: Path,
    custody_csv: Optional[Path] = None,
    nbim_csv: Optional[Path] = None,
    out_csv: Path = Path("breaks_llm.csv"),
    playbook_path: Optional[Path] = Path("llm_playbook.txt"),
    model: str = DEFAULT_MODEL,
    max_cost_usd: float = MAX_COST_USD,
) -> Path:
    """Annotate strict breaks with LLM categories/explanations/actions.
    
    Parameters
    ----------
    breaks_csv : Path
        The strict output CSV (must include COAC_EVENT_KEY and BANK_ACCOUNTS columns).
    custody_csv, nbim_csv : Path | None
        Original input files, used to provide full row-level context to the model.
    out_csv : Path
        Where to write the enriched CSV.
    playbook_path : Path | None
        Optional SOP that nudges the model to consistent, conservative behavior.
    model : str
        OpenAI model name (override via env if preferred).
    max_cost_usd : float
        Per-run budget cap; once estimated cost would exceed this, remaining groups are skipped.
    """
    # Read strict breaks
    breaks_df = pd.read_csv(breaks_csv, dtype=str)
    for c in [KEY_COAC, KEY_BANK]:
        if c not in breaks_df.columns:
            raise ValueError(f"breaks CSV missing '{c}' column.")

    # Load and normalize context files (if provided). Normalization harmonizes dates/numbers/currencies.
    custody_df = normalize_dataframe(read_csv_smart(Path(custody_csv))) if custody_csv else None
    nbim_df    = normalize_dataframe(read_csv_smart(Path(nbim_csv))) if nbim_csv else None

    playbook = _load_playbook(playbook_path)

    # Track estimated spend (heuristic) to respect the soft budget cap
    spent = 0.0

    enriched_rows = []
    groups = breaks_df.groupby([KEY_COAC, KEY_BANK], dropna=False)

    for (coac, bank), g in groups:
        # For each break group, pull the relevant context rows (may be multiple on either side).
        custody_rows = (
            custody_df[(custody_df[KEY_COAC]==coac) & (custody_df[KEY_BANK]==bank)].to_dict(orient="records")
            if custody_df is not None else []
        )
        nbim_rows = (
            nbim_df[(nbim_df[KEY_COAC]==coac) & (nbim_df[KEY_BANK]==bank)].to_dict(orient="records")
            if nbim_df is not None else []
        )
        breaks_rows = g.to_dict(orient="records")

        # System & user payload: this structure is easy to log/audit.
        system = (
            "You are a diligent operations analyst for a sovereign wealth fund. "
            "Analyze reconciliation breaks between CUSTODY and NBIM data and propose clear, conservative actions. "
            "Follow the playbook. Respond ONLY with a JSON object matching the schema."
        )
        user_payload = {
            "playbook": playbook,
            "key": {KEY_COAC: coac, KEY_BANK: bank},
            "breaks": breaks_rows,
            "custody_rows": custody_rows,
            "nbim_rows": nbim_rows,
            "schema": {
                "type": "object",
                "properties": {
                    "category": {"type":"string","enum": sorted(list(ALLOWED_CATEGORIES))},
                    "severity": {"type":"string","enum": sorted(list(ALLOWED_SEVERITY))},
                    "explanation": {"type":"string"},
                    "proposed_actions": {"type":"array","items":{"type":"string"}},
                    "custodian_email_draft": {"type":"string"}
                },
                "required": ["category","severity","explanation"]
            }
        }
        messages = [
            {"role":"system","content": system},
            {"role":"user","content": json.dumps(user_payload, ensure_ascii=False)}
        ]

        # --- Budget pre-check (conservative) ---------------------------------
        # Roughly estimate prompt+output tokens and associated cost
        prompt_tokens = est_tokens(system) + est_tokens(json.dumps(user_payload))
        est_prompt_cost = (prompt_tokens / 1000.0) * INPUT_PER_1K
        est_output_tokens = 500  # reserve some output budget to be safe
        est_output_cost = (est_output_tokens / 1000.0) * OUTPUT_PER_1K
        if spent + est_prompt_cost + est_output_cost > max_cost_usd:
            enriched_rows.append({
                KEY_COAC: coac, KEY_BANK: bank,
                "category": "Unknown",
                "severity": "MEDIUM",
                "explanation": "Skipped due to per-run budget limit.",
                "proposed_actions": "[]",
                "custodian_email_draft": ""
            })
            continue
        # ---------------------------------------------------------------------

        # Call the model; parse and validate JSON; update budget
        try:
            json_text = _ask_llm(messages, model=model)
            obj = json.loads(json_text)
        except Exception as e:
            # Defensive default to keep the pipeline moving
            obj = {"category": "Unknown", "severity": "MEDIUM", "explanation": f"LLM error: {e}"}

        obj = _validate_payload(obj)
        out_tokens = est_tokens(json.dumps(obj))
        spent += est_prompt_cost + (out_tokens/1000.0)*OUTPUT_PER_1K

        enriched_rows.append({
            KEY_COAC: coac, KEY_BANK: bank,
            "category": obj["category"],
            "severity": obj["severity"],
            "explanation": obj["explanation"],
            "proposed_actions": json.dumps(obj.get("proposed_actions", []), ensure_ascii=False),
            "custodian_email_draft": obj.get("custodian_email_draft","")
        })

    # Persist the enriched table
    out_df = pd.DataFrame(enriched_rows)
    out_df.to_csv(out_csv, index=False)
    return Path(out_csv)
