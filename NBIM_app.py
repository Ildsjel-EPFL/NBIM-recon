"""NBIM_app.py
Gradio UI for strict reconciliation + automatic LLM enrichment.

What this app does (end-to-end):
1) Accepts two CSV uploads (Custody & NBIM). The CSVs may be semicolon-delimited.
2) Runs a deterministic/strict reconciliation producing a breaks CSV (breaks_flags.csv).
3) Automatically runs an LLM-based enrichment on the breaks to classify / explain / propose actions.
4) Displays both strict and LLM tables in the UI and exposes download paths for each CSV.

Notes:
- We load environment variables (e.g., OPENAI_API_KEY) via python-dotenv so the LLM stage can run.
- The 'strict' stage handles locale-aware numbers and date normalization via utils_io.
- The LLM stage is budget-capped and uses JSON mode to produce structured outputs.
-----------------------------------------------------------------------------"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import gradio as gr

from strict_breaks_reconciliation import reconcile_breaks
from nbim_llm_breaks import run_llm_break_analysis

# Load the OpenAI API Key (and other envs) from a local .env file, if present.
from dotenv import load_dotenv
load_dotenv()


def run_strict(custody_file, nbim_file):
    """Run the strict comparator and return (DataFrame, status_message, breaks_csv_path, custody_path, nbim_path).
    
    Gradio calls this with two UploadedFile objects. We convert them to pathlib.Path,
    run the deterministic reconciliation, then read the resulting CSV for display.
    """
    if custody_file is None or nbim_file is None:
        return None, "Please upload both Custody and NBIM CSV files.", None, None, None

    # Where the uploaded temp files live on disk (Gradio handles the temp pathing)
    custody_path = Path(custody_file.name)
    nbim_path = Path(nbim_file.name)

    # The strict comparator writes to this path next to the upload
    out_path = custody_path.parent / "breaks_flags.csv"
    try:
        out_csv = reconcile_breaks(custody_path, nbim_path, out_path)
        df = pd.read_csv(out_csv)
        # Return the table to render, a status line, the path to CSV, and echo the inputs
        return df, f"Saved: {out_csv}", str(out_csv), custody_path, nbim_path
    except Exception as e:
        # Return a clear error message (and clear the extra outputs)
        return None, f"Error: {e}", None, None, None


def run_llm(breaks_csv_path, custody_path, nbim_path, budget_usd, model):
    """Run the LLM enrichment on the strict CSV and return (DataFrame, status_message, llm_csv_path).
    
    This is chained to run automatically right after 'run_strict'. We pass the same
    data files for row-level context so the LLM can reason with full evidence.
    """
    if not breaks_csv_path:
        return None, "Run the strict reconciliation first.", None

    out_path = Path(breaks_csv_path).parent / "breaks_llm.csv"
    try:
        out_csv = run_llm_break_analysis(
            breaks_csv=Path(breaks_csv_path),
            custody_csv=custody_path,
            nbim_csv=nbim_path,
            out_csv=out_path,
            model=model,
            max_cost_usd=float(budget_usd),
        )
        df = pd.read_csv(out_csv)
        return df, f"Saved: {out_csv}", str(out_csv)
    except Exception as e:
        return None, f"Error: {e}", None


# ---------------------------
# Gradio UI wiring
# ---------------------------
with gr.Blocks(title="NBIM Dividend Reconciliation") as demo:
    gr.Markdown(
        "## NBIM Dividend Reconciliation\n"
        "Upload Custody & NBIM CSVs. Click **Run Strict Compare** — the LLM Enrichment "
        "will run automatically right after the strict breaks appear."
    )

    # CSV uploads
    with gr.Row():
        custody_in = gr.File(label="Custody CSV", file_types=[".csv"])  # semicolons supported under the hood
        nbim_in    = gr.File(label="NBIM CSV", file_types=[".csv"])

    # LLM config row
    with gr.Row():
        budget = gr.Number(value=15, label="Max LLM budget (USD)")  # soft cap; the LLM step will skip groups if exceeded
        model  = gr.Textbox(value="gpt-4o-mini", label="Model")     # can be overridden in .env too

    # Strict comparator outputs
    strict_btn = gr.Button("Run Strict Compare")
    strict_status = gr.Markdown("")  # will show 'Saved: <path>' or error
    strict_table  = gr.Dataframe(headers=None, wrap=True, label="Breaks (strict)")
    strict_dl     = gr.Textbox(label="breaks_flags.csv path", interactive=False)

    # Hidden holders to pass input file paths into the LLM step automatically
    custody_path_box = gr.Textbox(visible=False)
    nbim_path_box    = gr.Textbox(visible=False)

    # LLM outputs (auto-run)
    llm_status = gr.Markdown("")
    llm_table  = gr.Dataframe(headers=None, wrap=True, label="Breaks (LLM categories)")
    llm_dl     = gr.Textbox(label="breaks_llm.csv path", interactive=False)

    # Clicking strict kicks off strict; THEN we chain .then(...) to auto-run the LLM
    strict_btn.click(
        run_strict,
        inputs=[custody_in, nbim_in],
        outputs=[strict_table, strict_status, strict_dl, custody_path_box, nbim_path_box]
    ).then(
        run_llm,
        inputs=[strict_dl, custody_path_box, nbim_path_box, budget, model],
        outputs=[llm_table, llm_status, llm_dl]
    )


if __name__ == "__main__":
    # Launch the Gradio server (defaults to localhost; set share=True if you need a public URL during demos)
    demo.launch()
