"""
utils/evaluate_tables.py
------------------------

LLM triage pipeline
~~~~~~~~~~~~~~~~~~~
1.  Look in  ``slr_tables/slr_tables_text_files/``   for every *.txt table
    produced by *get_pmc_tables.py*.
2.  Send the **entire** table text (max 4 000 chars) to GPT‑4o and ask a
    strict yes/no question:
        • Does the table include *all three* of —
              (1) a reference/citation column
              (2) a model‑type / model‑structure column
              (3) a cost / price column ?
3.  Depending on the answer:
        YES  →  copy the file to  ``kept_tables/``
        NO   →  copy the file to  ``discarded_tables/``

No logic has been changed — only comments, clearer variable names and a
few blank lines were added.
"""

from pathlib import Path
import os
import shutil
import time

import openai
from dotenv import load_dotenv


# --------------------------------------------------------------------------- #
#                           Load the OpenAI API key                           #
# --------------------------------------------------------------------------- #
load_dotenv()                                # read .env if present
openai.api_key = os.getenv("OPENAI_API_KEY") # required for GPT‑4o

if not openai.api_key:
    raise RuntimeError("OPENAI_API_KEY missing")


# --------------------------------------------------------------------------- #
#                Folder names (relative to  output_db/slr_tables)             #
# --------------------------------------------------------------------------- #
RAW_DIR_NAME  = "slr_tables_text_files"   # where *all* .txt tables land first
KEEP_DIR_NAME = "kept_tables"             # tables that satisfy our rule
DISC_DIR_NAME = "discarded_tables"        # everything else


# --------------------------------------------------------------------------- #
#                       Prompt used for every GPT call                        #
# --------------------------------------------------------------------------- #
SYSTEM_PROMPT = (
    "You are an expert in health‑economic systematic reviews.\n"
    "Does the table text contain *all three* of these columns?\n"
    "  1. References / citation of studies\n"
    "  2. Model type or model structure\n"
    "  3. Cost or price\n"
    "Reply ONLY with 'yes' or 'no'."
)


# --------------------------------------------------------------------------- #
#                   Ask GPT‑4o if a table is relevant or not                  #
# --------------------------------------------------------------------------- #
def _relevant(table_text: str, retries: int = 3) -> bool:
    """
    Return True  → keep  |  False → discard

    A small exponential‑backoff retry loop guards against transient API
    failures or rate limits.
    """
    for attempt in range(retries):
        try:
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": table_text[:4000]},
                ],
                temperature=0.0,
                max_tokens=5,
            )
            answer = response.choices[0].message.content.strip().lower()
            return answer.startswith("yes")

        except Exception as exc:
            print(f"⚠️  LLM error ({exc}); retrying…")
            time.sleep(2 ** attempt)   # 1 s, 2 s, 4 s …

    # If we exhausted our retries, play it safe and discard
    return False


# --------------------------------------------------------------------------- #
#                         Public helper — main workhorse                      #
# --------------------------------------------------------------------------- #
def triage_tables(slr_tables_root: str) -> None:
    """
    Walk through every *.txt in  <root>/slr_tables_text_files/  and copy it
    into either  kept_tables/  or  discarded_tables/  based on GPT‑4o.
    """
    root_path   = Path(slr_tables_root).resolve()
    raw_dir     = root_path / RAW_DIR_NAME
    keep_dir    = root_path / KEEP_DIR_NAME
    discard_dir = root_path / DISC_DIR_NAME

    keep_dir.mkdir(exist_ok=True)
    discard_dir.mkdir(exist_ok=True)

    # --- iterate over the harvested tables ---------------------------------
    for txt_path in raw_dir.glob("*.txt"):
        table_text = txt_path.read_text(encoding="utf-8")

        is_keep   = _relevant(table_text)
        dest_dir  = keep_dir if is_keep else discard_dir

        shutil.copy2(txt_path, dest_dir / txt_path.name)

        status = "✅ KEEP" if is_keep else "❌ DISC"
        print(f"{status}  {txt_path.name}")


# --------------------------------------------------------------------------- #
#                       CLI convenience  ->  python evaluate_tables.py        #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import sys

    # Allow an optional command‑line argument; otherwise default to
    #  ../output_db/slr_tables   relative to this file.
    root_arg = (
        sys.argv[1]
        if len(sys.argv) > 1
        else Path(__file__).parent.parent / "output_db" / "slr_tables"
    )

    triage_tables(str(root_arg))
