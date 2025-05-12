import os
import csv
from utils.step4_slr import collect_slr_from_csv, extract_tables_from_xml_dir, filter_and_generate
import csv
import json
import shutil
import io
import os
import sys
import time
from pathlib import Path
from typing import Iterable

# =============================== orchestrator ===============================
def main():
    # -------- input CSV --------------------------------------------------
    if len(sys.argv) > 1:
        input_csv = Path(sys.argv[1]).resolve()
        if not input_csv.exists():
            print("❌ supplied CSV path not found"); return
        slr_root = input_csv.parent / "slr_tables"
    else:
        base = Path(__file__).resolve().parent / "output_db"
        input_csv = base / "index_db" / "index_db.csv"
        slr_root  = base / "slr_tables"

    print(f"🗂  Using input CSV → {input_csv}")
    slr_root.mkdir(parents=True, exist_ok=True)

    # -------- run phases -------------------------------------------------
    collect_slr_from_csv(input_csv, slr_root)
    extract_tables_from_xml_dir(slr_root)
    filter_and_generate(slr_root)

    print("Step-4 pipeline complete!")


if __name__ == "__main__":
    main()

