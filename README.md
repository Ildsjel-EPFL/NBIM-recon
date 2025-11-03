# NBIM Dividend Reconciliation (Strict + LLM)

A small, production-minded demo that reconciles **Custody** vs **NBIM** dividend data using a **deterministic (strict)** pass and an **LLM enrichment** pass.  
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
