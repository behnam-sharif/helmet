#!/usr/bin/env python
"""
Step 4 wrapper:
  4a) build slr_papers.csv
  4b) download every table (get_pmc_tables)
  4c) LLM triage (evaluate_tables)
"""

import os
from utils.step4a_get_pmc_tables import collect_slr_to_csv, batch_from_csv
from utils.step4b_evaluate_tables import triage_tables

BASE = os.path.dirname(os.path.abspath(__file__))

# 4a – build list of SLR PMCIDs
PAPER_STORE = os.path.join(BASE, "output_db", "paper_storage")
SLR_CSV     = os.path.join(BASE, "output_db", "slr_papers.csv")
print("▶ Step 4a: collecting SLR papers…")
collect_slr_to_csv(PAPER_STORE, SLR_CSV)

# 4b – download tables
TABLE_ROOT = os.path.join(BASE, "output_db", "slr_tables")
print(f"\n▶ Step 4b: downloading tables to {TABLE_ROOT}")
batch_from_csv(SLR_CSV, TABLE_ROOT)

# 4c – LLM triage
print(f"\n▶ Step 4c: LLM triage of tables in {TABLE_ROOT}")
triage_tables(TABLE_ROOT)

print("\n✅  Step 4 complete!")
