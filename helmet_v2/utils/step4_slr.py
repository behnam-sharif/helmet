#!/usr/bin/env python
"""
Step4_slrDb.py
==============

End-to-end “Step 4” pipeline:

1. Harvest JSON + XML for every SLR article in an *input* CSV
2. Extract all <table-wrap> elements to text
3. Keep only tables that contain BOTH an author column AND a year/date column
4. Ask GPT-4o one question per kept table → slr_db.csv

Folder layout created
---------------------
output_db/
└─ slr_tables/
   ├─ slr_json/                 ← raw JSON from NCBI
   ├─ slr_xml/                  ← raw JATS XML
   ├─ slr_tables_text_files/    ← all pipe-delimited tables
   ├─ kept_tables/              ← after author+date filter
   ├─ discarded_tables/         ← filtered out
   └─ slr_db.csv                ← document,question,answer
"""

from __future__ import annotations

import csv
import json
import shutil
import io
import os
import sys
import time
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup
from lxml import etree

import openai
from dotenv import load_dotenv

# ------------------------------------------------------------------ config
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

EFETCH_XML  = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    "?db=pmc&id={pmcid}&retmode=xml"
)
EFETCH_JSON = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    "?db=pmc&id={pmcid}&retmode=json"
)

RAW_DIR_NAME   = "slr_tables_text_files"
KEEP_DIR_NAME  = "kept_tables"
DISC_DIR_NAME  = "discarded_tables"
DB_CSV_NAME    = "slr_db.csv"

AUTHOR_KEYS = ("author", "authors", "first author","Citation")
DATE_KEYS   = ("year", "date", "publication", "published")

SYSTEM_PROMPT = (
    "You are an assistant helping with health-economic SLR tables."
)
USER_PROMPT_TMPL = (
    "TABLE:\n{table}\n\n"
    "Answer the following question using the table above:\n"
    "\"{question}\"\n\n"
    "Return exactly one valid JSON object using this format:\n"
    "{{\n"
    "  \"question\": \"{question}\",\n"
    "  \"answer\": [\n"
    "    {{\"Author A\": \"answer specific to Author A\"}},\n"
    "    {{\"Author B\": \"answer specific to Author B\"}}\n"
    "  ]\n"
    "}}\n\n"
    "Use the actual author names from the table (typically the first column).\n"
    "Only return valid JSON. Do not include markdown formatting (no ```). Do not explain anything."
)
FILTER_TABLE_PROMPT = (
    "You are an assistant helping to decide whether to keep or discard a table "
    "extracted from a health-economic article.\n\n"
    "Each table includes a header row and 1 or more rows of data.\n"
    "Here is one table (pipe-delimited):\n\n"
    "{table}\n\n"
    "✅ KEEP the table **only if**:\n"
    "- It contains a column with author names, citations, or publication year.\n"
    "- It includes information relevant to economic modeling or cost-effectiveness analysis, such as:\n"
    "  cost, ICER, perspective, model type, time horizon, discount rate, comparator, etc.\n\n"
    "❌ DISCARD if:\n"
    "- It appears clinical only (e.g., lab results, outcomes, trial arms).\n"
    "- It has only 1 row of data (not including header).\n"
    "- It lacks both citation/author/year and model-related columns.\n\n"
    "Respond only with:\n"
    '{{ "keep": true }}   or   {{ "keep": false }}\n\n'
    "Only return valid JSON. No explanation or markdown."
)





def _generate_questions_via_gpt(headers: list[str]) -> list[str]:
    """Use GPT to generate one natural-language question per column header."""
    header_list = "\n".join(f"- {h}" for h in headers)
    prompt = (
    "Below is a list of table column headers from a health-economic study.\n"
    "Your task is to write **one clear, natural-language question** for each header, "
    "which can be answered based on that column.\n"
    "⚠️ Important: Use **plural phrasing** — assume each row represents a study.\n"
    "Use phrases like 'these studies', 'do', 'are', 'authors', etc. — NOT 'the study', 'does', or 'is'.\n\n"
    f"HEADERS:\n{header_list}\n\n"
    "Return your output as a JSON list of objects like this:\n"
    '[ {"column": "Perspective", "question": "What perspectives are used in these studies?"},\n'
    '  {"column": "Country", "question": "Which countries are covered in these studies?"} ]\n\n'
    "Do not explain anything. Only return valid JSON. No markdown formatting."
)

    for retry in range(3):
        try:
            r = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an assistant that writes natural-language questions for dataset headers."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=800,
            )

            response = r.choices[0].message.content.strip()

            if response.startswith("```json"):
                response = response.removeprefix("```json").removesuffix("```").strip()
            elif response.startswith("```"):
                response = response.removeprefix("```").removesuffix("```").strip()

            data = json.loads(response)
            return [item["question"] for item in data if "question" in item]

        except Exception as e:
            print(f"⚠️  GPT failed to generate questions from headers → {e}")
            time.sleep(2 ** retry)

    return []

# ---------------------------- OpenAI key -----------------------------------
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    print("❌  OPENAI_API_KEY not found in environment"); sys.exit(1)


# =============================== STEP 4-a ===================================
def _fetch_json_xml(pmcid: str, json_dir: Path, xml_dir: Path) -> None:
    """Download JSON + XML for one PMCID."""
    json_dir.mkdir(exist_ok=True)
    xml_dir.mkdir(exist_ok=True)

    j_path = json_dir / f"PMC{pmcid}.json"
    x_path = xml_dir  / f"PMC{pmcid}.xml"

    if not j_path.exists():
        rj = requests.get(EFETCH_JSON.format(pmcid=pmcid), headers=HEADERS, timeout=20)
        if rj.ok:
            j_path.write_text(rj.text, encoding="utf-8")

    if not x_path.exists():
        rx = requests.get(EFETCH_XML.format(pmcid=pmcid), headers=HEADERS, timeout=20)
        if rx.ok and b"<article" in rx.content:
            x_path.write_bytes(rx.content)
        else:
            print(f"⚠️  PMC{pmcid}: XML not available")


def collect_slr_from_csv(input_csv: Path, slr_root: Path) -> None:
    """Populate slr_json/ and slr_xml/ by hitting NCBI once per SLR PMCID."""
    json_dir = slr_root / "slr_json"
    xml_dir  = slr_root / "slr_xml"

    count = 0
    with input_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if not row.get("type", "").lower().startswith("slr_"):
                continue
            pmcid = str(row.get("pmcid", "")).strip()
            if pmcid:
                _fetch_json_xml(pmcid, json_dir, xml_dir)
                count += 1
                time.sleep(0.34)   # polite pause
    print(f"✅  4-a: fetched JSON/XML for {count} SLR articles")


# =============================== STEP 4-b ===================================
def _tables_from_xml_file(x_path: Path):
    """Yield (table_id, pipe-delimited-text) from one XML file, including styled/nested text."""
    xml = etree.parse(str(x_path))
    for idx, wrap in enumerate(xml.findall(".//table-wrap")):
        tid = (wrap.findtext("label") or f"T{idx+1}").strip().replace(" ", "_")
        tbl = wrap.find(".//table")
        if tbl is None:
            continue

        # Use itertext() to extract text inside any nested elements (e.g., <b>, <i>, <xref>)
        hdrs = [
            "".join(th.itertext()).strip()
            for th in tbl.findall(".//thead//th")
        ]

        rows = []
        for tr in tbl.findall(".//tbody//tr"):
            row = [
                "".join(td.itertext()).strip()
                for td in tr.findall("td")
            ]
            rows.append(row)

        # Write to pipe-delimited buffer
        buf = io.StringIO()
        w = csv.writer(buf, delimiter="|")
        if hdrs:
            w.writerow(hdrs)
        w.writerows(rows)
        yield tid, buf.getvalue()



def extract_tables_from_xml_dir(slr_root: Path) -> None:
    """Write every JATS table into slr_tables_text_files/."""
    xml_dir = slr_root / "slr_xml"
    raw_dir = slr_root / RAW_DIR_NAME
    raw_dir.mkdir(exist_ok=True)

    saved = 0
    for xml_file in xml_dir.glob("PMC*.xml"):
        pmcid = xml_file.stem.replace("PMC", "")
        for tid, txt in _tables_from_xml_file(xml_file):
            (raw_dir / f"{pmcid}_{tid}.txt").write_text(txt, encoding="utf-8")
            saved += 1
    print(f"✅  4-b: extracted {saved} tables from XML")


# =============================== STEP 4-c/d =================================
def _parse_headers(text: str) -> list[str]:
    """Return first line→headers (lower-case)."""
    first = text.splitlines()[0]
    return [h.strip().lower() for h in first.split("|")]

def _gpt_should_keep_table(table_text: str) -> bool:
    prompt = FILTER_TABLE_PROMPT.format(table=table_text[:3500])  # truncate just in case

    for retry in range(3):
        try:
            r = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a table filtering assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,
                max_tokens=100,
            )
            response = r.choices[0].message.content.strip()

            if response.startswith("```json"):
                response = response.removeprefix("```json").removesuffix("```").strip()
            elif response.startswith("```"):
                response = response.removeprefix("```").removesuffix("```").strip()

            result = json.loads(response)
            return result.get("keep", False)

        except Exception as e:
            print(f"⚠️ GPT table filtering failed: {e}")
            time.sleep(2 ** retry)

    return False  # default to discard if GPT fails

def _has_author_and_date(headers: Iterable[str]) -> bool:
    has_author = any(k in col for col in headers for k in AUTHOR_KEYS)
    has_date   = any(k in col for col in headers for k in DATE_KEYS)
    return has_author and has_date


def _ask_llm(table_text: str, question: str) -> tuple[str, str] | None:
    """Ask GPT a specific question about a table and return (question, answer)."""
    
    prompt = USER_PROMPT_TMPL.format(table=table_text[:4000], question=question)

    for retry in range(3):
        try:
            r = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                max_tokens=800,
            )

            response = r.choices[0].message.content.strip()

            # Strip ``` wrappers
            if response.startswith("```json"):
                response = response.removeprefix("```json").removesuffix("```").strip()
            elif response.startswith("```"):
                response = response.removeprefix("```").removesuffix("```").strip()

            if not response.startswith("{"):
                print(f"⚠️  Unexpected GPT output (not JSON):\n{response[:500]}...")
                return None

            # Try full JSON parse
            try:
                data = json.loads(response)
                answer = data.get("answer", "")
                if isinstance(answer, list):
                    if all(isinstance(item, dict) for item in answer):
                        answer = "; ".join(f"{k}: {v}" for d in answer for k, v in d.items())
                    else:
                        answer = "; ".join(str(a) for a in answer)
                return question, str(answer)

            except json.JSONDecodeError as e:
                print(f"⚠️  JSON decode error: {e}")
                print(f"🔍 Raw GPT output (partial):\n{response[:500]}...")

                # Try partial recovery from malformed answer block
                if "answer" in response:
                    try:
                        start = response.find('[')
                        end = response.rfind('}')
                        trimmed = response[start:end+1] + "]"
                        partial = json.loads(trimmed)
                        if all(isinstance(item, dict) for item in partial):
                            answer = "; ".join(f"{k}: {v}" for d in partial for k, v in d.items())
                            return question, answer
                    except Exception as fallback_error:
                        print(f"⚠️  Fallback JSON parse failed: {fallback_error}")
                        return None

        except Exception as exc:
            print(f"⚠️  LLM error ({exc}); retry {retry+1}/3")
            time.sleep(2 ** retry)

    return None

def filter_and_generate(slr_root: Path) -> None:
    """
    For each table:
    - Keep/discard by header heuristic
    - Ask GPT multiple questions
    - Use the answer to "Who are the authors..." as the 'document' value
    - Write full slr_db.csv
    """
    raw_dir   = slr_root / RAW_DIR_NAME
    keep_dir  = slr_root / KEEP_DIR_NAME
    disc_dir  = slr_root / DISC_DIR_NAME
    db_csv    = slr_root / DB_CSV_NAME

    keep_dir.mkdir(exist_ok=True)
    disc_dir.mkdir(exist_ok=True)
    db_csv.parent.mkdir(parents=True, exist_ok=True)

    all_rows = []

    for txt_file in raw_dir.glob("*.txt"):
        lines = txt_file.read_text(encoding="utf-8").splitlines()
        if len(lines) < 2:
            print(f"⚠️  Skipping empty or malformed table: {txt_file.name}")
            continue

        headers = _parse_headers(lines[0])
        table_name = txt_file.stem
        text = "\n".join(lines)

        if _gpt_should_keep_table(text):
            shutil.copy2(txt_file, keep_dir / txt_file.name)
            print(f"✅ KEEP  {table_name}")

            matched_questions = _generate_questions_via_gpt(headers)

            if not matched_questions:
                print("   ⚠️  No matching questions for this table")
                continue

            table_rows = []
            author_string = None

            for q in matched_questions:
                qa = _ask_llm(text, q)
                if qa:
                    if "who are the authors" in q.lower():
                        author_string = qa[1]
                    table_rows.append((q, qa[1]))
                    print(f"   ↳ Q: {q} ✓")
                else:
                    print(f"   ⚠️  GPT failed for question: {q}")

            for q, a in table_rows:
                all_rows.append({
                    "slr_tbl": table_name,
                    "document": author_string or "",
                    "question": q,
                    "answer": a
                })

        else:
            shutil.copy2(txt_file, disc_dir / txt_file.name)
            print(f"❌ DISC  {table_name}")

    # Write final CSV
    with db_csv.open("w", newline="", encoding="utf-8") as db_handle:
        writer = csv.DictWriter(db_handle, fieldnames=["slr_tbl", "document", "question", "answer"])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\n✅  4-c/d complete: {db_csv.name} written with GPT-derived document column.")
