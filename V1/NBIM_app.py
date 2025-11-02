"""
Gradio UI for Dividend Reconciliation (Local)
---------------------------------------------
Upload Custody & NBIM CSVs → get the final breaks_analysis.csv
and display its content with each row formatted as:

[COLUMN_NAME] : value

(with a blank line between rows)
"""

import os
import shutil
from pathlib import Path
from datetime import datetime
from uuid import uuid4
import traceback
import pandas as pd
import gradio as gr

# Loading the OpenAI API Key from the .env file
from dotenv import load_dotenv
load_dotenv()

# Import the two pipeline scripts in the same folder:
try:
    import strict_breaks_reconciliation as sbr
except Exception as e:
    raise ImportError(
        "Could not import 'strict_breaks_reconciliation'. "
        "Ensure strict_breaks_reconciliation.py is in the same folder."
    ) from e

try:
    from nbim_llm_breaks import run_llm_break_analysis
except Exception as e:
    raise ImportError(
        "Could not import 'nbim_llm_breaks'. "
        "Ensure nbim_llm_breaks.py is in the same folder."
    ) from e


def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def _format_breaks_for_display(csv_path: Path) -> str:
    """
    Reads breaks_analysis.csv and returns a human-friendly multi-line string:
    [COLUMN] : value
    ...
    <blank line>
    [COLUMN] : value
    ...
    """
    if not csv_path.exists():
        return "No analysis file found."

    df = pd.read_csv(csv_path)
    if df.empty:
        return "breaks_analysis.csv is empty (no transactions analyzed)."

    lines = []
    # Preserve column order from the CSV
    cols = list(df.columns)

    for _, row in df.iterrows():
        for c in cols:
            val = row.get(c, "")
            # Render NaN as empty string
            val = "" if pd.isna(val) else str(val)
            lines.append(f"[{c}] : {val}")
        # blank line between breaks
        lines.append("")
    return "\n".join(lines)


def process_pipeline(custody_file, nbim_file, model_name, temperature):
    """
    Orchestrates the pipeline:
    - Saves uploads
    - strict reconciliation -> breaks_flags.csv
    - LLM analysis -> breaks_analysis.csv
    Returns:
      (analysis_file_path or None, logs_str, formatted_display_str)
    """
    log_lines = []
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_id = f"ui_run_{ts}_{uuid4().hex[:8]}"
    workdir = Path("./ui_runs") / run_id
    _ensure_dir(workdir)

    try:
        # Validate uploads
        if custody_file is None or nbim_file is None:
            return None, "❌ Please upload BOTH Custody and NBIM CSV files.", ""

        custody_path = workdir / "custody.csv"
        nbim_path = workdir / "nbim.csv"
        shutil.copyfile(custody_file, custody_path)
        shutil.copyfile(nbim_file, nbim_path)
        log_lines.append(f"📥 Inputs saved to: {workdir}")

        # Step 1: Strict breaks reconciliation
        breaks_flags_path = workdir / "breaks_flags.csv"
        log_lines.append("🔎 Running strict reconciliation ...")
        sbr.reconcile_breaks(custody_path, nbim_path, breaks_flags_path)
        log_lines.append(f"✅ Created: {breaks_flags_path.name}")

        # Step 2: LLM analysis -> breaks_analysis.csv
        if not os.getenv("OPENAI_API_KEY"):
            log_lines.append("⚠️ OPENAI_API_KEY not set — cannot run the LLM step.")
            log_lines.append("   Set it and rerun (Windows PowerShell): setx OPENAI_API_KEY \"sk-...\"")
            return None, "\n".join(log_lines), ""

        analysis_out = workdir / "breaks_analysis.csv"
        log_lines.append(f"🤖 Running LLM analysis: model={model_name}, T={temperature}")
        run_llm_break_analysis(
            breaks_flags_path=str(breaks_flags_path),
            out_csv=str(analysis_out),
            model=model_name,
            temperature=float(temperature),
        )
        log_lines.append(f"✅ Final file ready: {analysis_out.name}")
        log_lines.append("🎉 Done.")

        # Format for display
        pretty_text = _format_breaks_for_display(analysis_out)

        return str(analysis_out), "\n".join(log_lines), pretty_text

    except Exception as e:
        tb = traceback.format_exc()
        log_lines.append("❌ Error during processing:")
        log_lines.append(str(e))
        log_lines.append(tb)
        return None, "\n".join(log_lines), ""


# ---------------- Gradio UI ---------------- #
with gr.Blocks(title="Dividend Reconciliation – Local") as demo:
    gr.Markdown(
        """
        # Dividend Reconciliation (Local UI)
        Upload the **Custody** and **NBIM** CSVs.  
        Click **Run** to generate **breaks_analysis.csv** and preview its contents below.
        """
    )

    with gr.Row():
        custody_input = gr.File(label="Custody CSV", file_types=[".csv"])
        nbim_input = gr.File(label="NBIM CSV", file_types=[".csv"])

    with gr.Accordion("Advanced (LLM)", open=False):
        model_name = gr.Textbox(
            label="OpenAI model",
            value="gpt-4o-mini",
            info="OpenAI model used for explanations (requires OPENAI_API_KEY)",
        )
        temperature = gr.Slider(
            label="Temperature",
            minimum=0.0, maximum=1.0, step=0.1, value=0.0
        )

    run_btn = gr.Button("Run", variant="primary")

    with gr.Row():
        analysis_file = gr.File(label="Download: breaks_analysis.csv", interactive=False)

    with gr.Row():
        # Large textbox to show formatted breaks with blank lines between them
        preview_box = gr.Textbox(
            label="Preview: breaks_analysis content",
            lines=24,
            interactive=False
        )

    logs_box = gr.Textbox(label="Logs", lines=12, interactive=False)

    def _on_run(custody, nbim, mdl, temp):
        out_path, logs, pretty_text = process_pipeline(custody, nbim, mdl, temp)
        return out_path, pretty_text, logs

    run_btn.click(
        _on_run,
        inputs=[custody_input, nbim_input, model_name, temperature],
        outputs=[analysis_file, preview_box, logs_box],
    )

if __name__ == "__main__":
    demo.launch(server_name="127.0.0.1", server_port=7860, inbrowser=True)
