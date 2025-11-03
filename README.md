# LLM-Powered Dividend Reconciliation System

A small demo that reconciles **Custody** vs **NBIM** dividend data using a **deterministic (strict)** pass and an **LLM enrichment** pass.  
It supports **semicolon-separated CSVs**, **locale-aware numbers**, **date normalization with day-first inference**, and an **explicit, auditable field mapping** between the two datasets.

> The UI (Gradio) runs the strict comparison first, then **automatically** runs the LLM enrichment and shows both results with download links.

---

## Features

- **Deterministic “strict” comparison** with playbook-aligned tolerances
  - Money: `±0.01`
  - Rates: `±1e-4`
  - Dates: exact equality after normalization to `YYYY-MM-DD`
  - Currencies: case-insensitive equality
- **Explicit, auditable column mapping** (no surprises from “intersection-of-columns”)
- **Semicolon-delimited CSV** support
- **Locale-aware numeric parsing** (e.g., `1,23` vs `1.23`, `1 234,56`, etc.)
- **Day-first inference** for ambiguous date formats
- **LLM enrichment** (JSON-mode, budget capped) to classify the break, explain, and propose actions + email draft
- **Custodian display names are ignored** during comparisons by design

---

## Environment variables (and OpenAI API key)

Update the `OPENAI_API_KEY` with your API key the `.env` file:

```env
OPENAI_API_KEY=sk-XXXX

# Optional overrides
LLM_MODEL=gpt-4o-mini
MAX_COST_USD=15
INPUT_COST_PER_1K=0.005
OUTPUT_COST_PER_1K=0.015
```

> The LLM stage uses **OpenAI JSON mode** for robust parsing and enforces a **soft per-run budget**.  
> If the next group would exceed the cap, that group is **skipped** with a clear note in the output.

---

## How it works

### 1) Strict pass (`strict_breaks_reconciliation.py`)
- Reads CSVs with **auto delimiter detection** (comma or semicolon).
- Normalizes:
  - Dates → `YYYY-MM-DD` (with **day-first** inference per column).
  - Numbers via **locale-aware** parsing.
  - Currencies to **upper-case**.
- Resolves the **join keys** with aliasing:
  - Custody: `COAC_EVENT_KEY` + `BANK_ACCOUNTS` (or `BANK_ACCOUNT`)
  - NBIM:    `COAC_EVENT_KEY` + `BANK_ACCOUNT` (or `BANK_ACCOUNTS`)
- Outer-joins on keys to find **missing on either side**.
- For keys present on both sides, compares the **explicit mapping** with **type-aware** logic and tolerances.
- Writes `breaks_flags.csv` with one row per break/missing key and a detailed **reason**.

### 2) LLM enrichment (`nbim_llm_breaks.py`)
- Groups strict breaks by `(COAC_EVENT_KEY, BANK_ACCOUNTS)`.
- For each group, passes:
  - The **playbook** (`llm_playbook.txt`),
  - The **break rows** from strict,
  - The **full row context** from both datasets.
- Uses **OpenAI JSON mode** to return a single JSON object per group:
  - `category` ∈ {Rounding, FX, Tax, Data entry error, Missing booking, Corporate action nuance, Unknown}
  - `severity` ∈ {LOW, MEDIUM, HIGH}
  - `explanation`, `proposed_actions[]`, `custodian_email_draft`
- Writes `breaks_llm.csv`.

### 3) UI (`NBIM_app.py`)
- Upload files → click **Run Strict Compare**.
- Shows the strict table and **auto-runs** the LLM step.
- Shows the enriched table, with **download paths** for both CSVs.

---

## Explicit mapping (Custody → NBIM)

The project compares **exactly** these pairs for each matched `(COAC_EVENT_KEY, BANK_ACCOUNT(S))`.  
Header **aliases** are supported (e.g., `EX_DATE` ↔ `EXDATE`, `BANK_ACCOUNT` ↔ `BANK_ACCOUNTS`).

| Custody            | NBIM                      | Type      |
|--------------------|---------------------------|-----------|
| COAC_EVENT_KEY     | COAC_EVENT_KEY            | text      |
| BANK_ACCOUNTS      | BANK_ACCOUNT              | text      |
| ISIN               | ISIN                      | text      |
| SEDOL              | SEDOL                     | text      |
| NOMINAL_BASIS      | NOMINAL_BASIS             | text      |
| EX_DATE            | EXDATE                    | date      |
| PAY_DATE           | PAYMENT_DATE              | date      |
| CURRENCIES         | QUOTATION_CURRENCY        | currency  |
| DIV_RATE           | DIVIDENDS_PER_SHARE       | rate      |
| TAX_RATE           | WTHTAX_RATE               | rate      |
| GROSS_AMOUNT       | GROSS_AMOUNT_QUOTATION    | money     |
| NET_AMOUNT_QC      | NET_AMOUNT_QUOTATION      | money     |
| TAX                | WTHTAX_COST_QUOTATION     | money     |
| NET_AMOUNT_SC      | NET_AMOUNT_SETTLEMENT     | money     |
| SETTLED_CURRENCY   | SETTLEMENT_CURRENCY       | currency  |

> Keys are resolved with robust aliases on **both** sides:
> - NBIM may provide `BANK_ACCOUNTS` instead of `BANK_ACCOUNT` (and vice versa); both are recognized.
> - Same for `EX_DATE`/`EXDATE`, `PAY_DATE`/`PAYMENT_DATE`, etc.

---

## Prompt and playbook 
Prompt for each OpenAI API call:
```markdown
"You are a diligent operations analyst for a sovereign wealth fund. "
"Analyze reconciliation breaks between CUSTODY and NBIM data and propose clear, conservative actions. "
"Follow the playbook. Respond ONLY with a JSON object matching the schema."
```
Playbook for SOP:
```markdown
1) Monetary amounts: treat differences up to +/- 0.01 (in the booking currency) as non-issues. Anything larger requires explanation.
2) Shares/quantities: should match; micro rounding up to 1e-6 can be tolerated only if justified (e.g., fractional entitlements).
3) Dates: EX/RECORD/PAYMENT dates must match exactly once normalized to YYYY-MM-DD.
4) FX & rates: ensure FX math explains amount deltas; quote the applicable FX if used.
5) Common legitimate reasons: rounding, FX conversions, ADR/withholding fees, market-specific tax rates.
6) When escalation is needed: propose concrete next actions and include a short, professional email draft to the custodian (subject + 2–4 sentences).
7) Keep recommendations conservative and auditable.
```

---

## Getting Started

1) **Install dependencies**
```bash
pip install -r requirements.txt
```

2) **Run the app**
```bash
python NBIM_app.py
```

3) **Use the UI**
- Upload the **Custody** and **NBIM** CSVs (semicolon `;` is supported automatically).
- Click **Run Strict Compare**.
- The app displays the strict **breaks table** and **automatically runs the LLM enrichment**, showing a second table with categories/explanations/actions and a custodian email draft.
- Both CSV outputs are listed with file paths for download:
  - `breaks_flags.csv`
  - `breaks_llm.csv`

---

## CLI (optional)

You can call the building blocks directly from Python:

```python
from pathlib import Path
from strict_breaks_reconciliation import reconcile_breaks
from nbim_llm_breaks import run_llm_break_analysis

# Strict reconciliation
strict_csv = reconcile_breaks(Path("custody.csv"), Path("nbim.csv"))

# LLM enrichment
enriched_csv = run_llm_break_analysis(
    breaks_csv=strict_csv,
    custody_csv=Path("custody.csv"),
    nbim_csv=Path("nbim.csv"),
    model="gpt-4o-mini",
    max_cost_usd=15,
)
```


## Security & privacy

- The LLM step sends only the necessary rows & metadata for the selected break groups.  
- Avoid uploading personally identifiable information; mask/redact if needed.  
- Use a separate API key with minimum privileges and rotate regularly.
