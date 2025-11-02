# NBIM Dividend Reconciliation (Strict + LLM)

A small, production-minded demo that reconciles **Custody** vs **NBIM** dividend data using a **deterministic (strict)** pass and an **LLM enrichment** pass.  
It supports **semicolon-separated CSVs**, **locale-aware numbers**, **date normalization with day-first inference**, and an **explicit, auditable field mapping** between the two datasets.

> The UI (Gradio) runs the strict comparison first, then **automatically** runs the LLM enrichment and shows both results with download links.

---

## ✨ Features

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

## 🗂 Repository layout

```
.
├── NBIM_app.py                    # Gradio UI: strict compare then auto LLM enrichment
├── strict_breaks_reconciliation.py# Deterministic reconciliation using explicit mapping
├── nbim_llm_breaks.py             # LLM pass: categories, severity, actions, email draft
├── utils_io.py                    # CSV delimiter detection, locale-aware numbers, date inference, synonyms
├── llm_playbook.txt               # Short SOP to steer the LLM
├── requirements.txt               # Pinned, compatible versions
└── README.md                      # This file
```

---

## 🔐 Environment variables

Create a `.env` file in the project root (or export in your shell):

```env
OPENAI_API_KEY=sk-...

# Optional overrides
LLM_MODEL=gpt-4o-mini
MAX_COST_USD=15
INPUT_COST_PER_1K=0.005
OUTPUT_COST_PER_1K=0.015
```

> The LLM stage uses **OpenAI JSON mode** for robust parsing and enforces a **soft per-run budget**.  
> If the next group would exceed the cap, that group is **skipped** with a clear note in the output.

---

## 🚀 Quickstart

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

## 🧮 Explicit mapping (Custody → NBIM)

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

## 🧱 How it works

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

## ⚙️ Configuration

- **Tolerances** (edit in `strict_breaks_reconciliation.py`):
  - `MONEY_TOL = 0.01`
  - `RATE_TOL = 1e-4`
- **Aliases / vendor header variants** (edit in `strict_breaks_reconciliation.py` under `ALIASES`).
- **Playbook** (edit `llm_playbook.txt` or replace via `--`/env path).
- **Model & budget**: override via `.env` or the UI fields.

---

## 🧪 CLI / Headless usage (optional)

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

---

## 🧯 Troubleshooting

- **“Missing required key column 'BANK_ACCOUNT'”**  
  NBIM may export `BANK_ACCOUNTS` instead of `BANK_ACCOUNT`. The resolver treats both as equivalent on **either** side. If you still see this, verify the header row is intact and not shifted by a wrong delimiter (the project auto-detects semicolons).

- **“Dates look swapped (MM/DD vs DD/MM)”**  
  The reader infers **day-first** per column. If your data is very sparse/ambiguous, set a fixed policy by coercing the date columns before calling strict.

- **“The LLM step stopped early”**  
  You likely hit the **budget cap**. Increase `MAX_COST_USD` in `.env` or the UI and re-run.

---

## 🔒 Security & privacy

- The LLM step sends only the necessary rows & metadata for the selected break groups.  
- Avoid uploading personally identifiable information; mask/redact if needed.  
- Use a separate API key with minimum privileges and rotate regularly.

---

## 📝 License

MIT — see `LICENSE` (or adapt to your policy).