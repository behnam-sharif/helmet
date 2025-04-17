"""
utils/get_pmc_tables.py
=======================

Grab *every* table from a PubMed Central (PMC) article, regardless of how
that table is stored (inline HTML, linked HTML, JATS‑XML, TSV download,
or raster image).

The extractor tries a series of progressively looser strategies until it
finds something:

    1. `<table-wrap>` elements in the JATS XML returned by EFetch
    2. `<div class="table-wrap">` elements in the “classic” HTML view
    3. `<a href="/table/...">` links in the same classic view
    4. Plain‑text TSV files listed by the PMC OA utility
    5. Blindly probe `/table/T1/`, `/table/T2/`, … until the server
       returns a 404 (catches hidden tables)

* Structured tables are written as pipe‑delimited `.txt` files to
  `…/slr_tables/slr_tables_text_files/`.

* Raster images (`<img>` only) are downloaded to the parent
  `…/slr_tables/` folder – you can OCR them later if needed.
"""

from __future__ import annotations

import csv
import io
import os
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from lxml import etree


# --------------------------------------------------------------------------- #
#                               Global settings                               #
# --------------------------------------------------------------------------- #

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"  # pretend to be a browser
}

# NCBI helper endpoints
EFETCH = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    "?db=pmc&id={pmcid}&retmode=xml"
)
IDCONV = (
    "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
    "?ids={pmcid}&format=xml"
)
OA_UTIL = (
    "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.py"
    "?id=PMC{pmcid}&format=xml"
)

RAW_DIR_NAME = "slr_tables_text_files"   # subfolder for *all* .txt tables


# --------------------------------------------------------------------------- #
#                       Level‑1:  Get the XML if available                    #
# --------------------------------------------------------------------------- #
def _xml_or_none(pmcid: str) -> str | None:
    """
    Try to retrieve the full JATS XML for `pmcid`.

    Returns
    -------
    xml_text | None
        None is returned when the publisher blocks XML download.
    """
    # 1) Direct EFetch
    r = requests.get(EFETCH.format(pmcid=pmcid), headers=HEADERS, timeout=15)
    if r.ok and b"<article" in r.content:
        return r.text

    # 2) Some journals require indirection via the "idconv" API
    conv = requests.get(IDCONV.format(pmcid=pmcid), headers=HEADERS, timeout=15)
    match = re.search(r'href=\"([^\"]+)\"', conv.text) if conv.ok else None
    if not match:
        return None

    xml_url = match.group(1) + "?output=xml"
    r2 = requests.get(xml_url, headers=HEADERS, timeout=15)
    return r2.text if r2.ok else None


def _tables_from_xml(xml_text: str):
    """
    Yield `(table_id, pipe_delimited_text)` tuples for every <table-wrap>
    inside the supplied XML.
    """
    root = etree.fromstring(xml_text.encode())
    for idx, tbl_wrap in enumerate(root.findall(".//table-wrap")):
        table_id = (tbl_wrap.findtext("label") or f"T{idx+1}").strip().replace(" ", "_")
        table_el = tbl_wrap.find(".//table")
        if table_el is None:
            continue

        # Capture header row (if present)
        headers = [
            th.text.strip() for th in table_el.findall(".//thead//th") if th.text
        ]

        # Capture each subsequent row
        body_rows = [
            [(cell.text or "").strip() for cell in row.findall("td")]
            for row in table_el.findall(".//tbody//tr")
        ]

        # Convert to a pipe‑delimited *markdown‑like* table
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="|")
        if headers:
            writer.writerow(headers)
        writer.writerows(body_rows)

        yield table_id, buf.getvalue()


# --------------------------------------------------------------------------- #
#              Level‑2:  Inline <div class="table-wrap"> in HTML              #
# --------------------------------------------------------------------------- #
def _classic_inline_tables(pmcid: str):
    """
    Crawl the classic HTML view and yield either
        ("TXT", table_id, text)   or
        ("IMG", table_id, image_url)
    """
    html_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmcid}/?report=classic"
    resp = requests.get(html_url, headers=HEADERS, timeout=15)
    if not resp.ok:
        return  # network error – silently skip
    soup = BeautifulSoup(resp.text, "html.parser")

    for idx, wrapper in enumerate(soup.select("div.table-wrap")):
        table_id = wrapper.get("id") or f"T{idx+1}"
        real_table = wrapper.find("table")

        # (a) fully structured
        if real_table:
            headers = [th.get_text(strip=True) for th in real_table.find_all("th")]
            rows = [
                [td.get_text(strip=True) for td in tr.find_all("td")]
                for tr in real_table.find_all("tr") if tr.find_all("td")
            ]
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="|")
            if headers:
                writer.writerow(headers)
            writer.writerows(rows)
            yield "TXT", table_id, buf.getvalue()
            continue

        # (b) raster image only
        img_tag = wrapper.find("img")
        if img_tag and img_tag.get("src"):
            src = img_tag["src"]
            absolute_src = "https://www.ncbi.nlm.nih.gov" + src if src.startswith("/") else src
            yield "IMG", table_id, absolute_src


# --------------------------------------------------------------------------- #
#                Level‑3:  Linked /table/ pages in classic HTML               #
# --------------------------------------------------------------------------- #
def _html_table_links(pmcid: str):
    """
    Return *every* href that looks like "/table/…" from the classic view.
    """
    classic = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmcid}/?report=classic"
    resp = requests.get(classic, headers=HEADERS, timeout=15)
    if not resp.ok:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = {
        "https://www.ncbi.nlm.nih.gov" + a["href"]
        for a in soup.select("a[href*='/table/']")
        if a["href"].startswith("/pmc/")
    }
    return sorted(links)


def _table_to_csv(table_url: str):
    """
    Convert the table at `table_url` to pipe‑delimited text.

    Returns
    -------
    (table_id, text) | (None, None)  on failure.
    """
    # `?report=objectonly` strips the page to just the table
    obj_url = table_url if table_url.endswith("?report=objectonly") else table_url + "?report=objectonly"
    resp = requests.get(obj_url, headers=HEADERS, timeout=15)
    if not resp.ok:
        return None, None

    soup = BeautifulSoup(resp.text, "html.parser")
    table_el = soup.find("table")
    if not table_el:                             # linked page might be an image
        return None, None

    headers = [th.get_text(strip=True) for th in table_el.find_all("th")]
    rows = [
        [td.get_text(strip=True) for td in tr.find_all("td")]
        for tr in table_el.find_all("tr") if tr.find_all("td")
    ]
    if not headers and not rows:
        return None, None

    table_id = table_url.rstrip("/").split("/")[-1]
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="|")
    if headers:
        writer.writerow(headers)
    writer.writerows(rows)
    return table_id, buf.getvalue()


def _attachment_image(table_url: str, out_dir: Path, pmcid: str):
    """
    If `table_url` contains only an <img>, download that image to `out_dir`.
    """
    resp = requests.get(table_url, headers=HEADERS, timeout=15)
    if not resp.ok:
        return
    soup = BeautifulSoup(resp.text, "html.parser")
    img_tag = soup.find("img")
    if not img_tag or not img_tag.get("src"):
        return

    src = img_tag["src"]
    absolute_src = "https://www.ncbi.nlm.nih.gov" + src if src.startswith("/") else src
    ext = os.path.splitext(absolute_src)[1] or ".png"
    fname = f"{pmcid}_{table_url.rstrip('/').split('/')[-1]}{ext}"
    (out_dir / fname).write_bytes(
        requests.get(absolute_src, headers=HEADERS, timeout=15).content
    )
    print(f"🖼️   saved {fname}")


# --------------------------------------------------------------------------- #
#            Level‑4:  PMC OA utility – look for plain‑text TSVs              #
# --------------------------------------------------------------------------- #
def _oa_tsv_tables(pmcid: str):
    resp = requests.get(OA_UTIL.format(pmcid=pmcid), headers=HEADERS, timeout=15)
    if not resp.ok:
        return
    soup = BeautifulSoup(resp.text, "xml")

    for file_el in soup.find_all("file", format="tsv"):
        href = file_el.get("href")
        if not href:
            continue
        tid = os.path.splitext(os.path.basename(href))[0]
        tsv_text = requests.get(href, headers=HEADERS, timeout=15).text
        # convert TSV → pipe‑delimited
        pipe_text = "\n".join(["|".join(line.split("\t")) for line in tsv_text.splitlines()])
        yield tid, pipe_text


# --------------------------------------------------------------------------- #
#        Level‑5:  Blindly probe /table/T1/, /table/T2/, … pattern            #
# --------------------------------------------------------------------------- #
def _probe_T_pages(pmcid: str, out_dir: Path, raw_dir: Path,
                   max_n: int = 30) -> int:
    """
    Probe sequential `/table/Tn/` pages until the first 404.

    Returns
    -------
    int
        Number of structured tables saved.
    """
    base = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmcid}/table/"
    saved_count = 0

    for n in range(1, max_n + 1):
        page_url = f"{base}T{n}/?report=objectonly"
        resp = requests.get(page_url, headers=HEADERS, timeout=15)

        if resp.status_code == 404:
            break                       # stop probing at first definite miss
        if not resp.ok:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        tid = f"T{n}"
        table_el = soup.find("table")

        # structured table
        if table_el:
            headers = [th.get_text(strip=True) for th in table_el.find_all("th")]
            rows = [
                [td.get_text(strip=True) for td in tr.find_all("td")]
                for tr in table_el.find_all("tr") if tr.find_all("td")
            ]
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="|")
            if headers:
                writer.writerow(headers)
            writer.writerows(rows)

            (raw_dir / f"{pmcid}_{tid}.txt").write_text(buf.getvalue(), encoding="utf-8")
            saved_count += 1
            print(f"📑  saved {pmcid}_{tid}.txt (probed)")
            continue

        # raster image
        img_tag = soup.find("img")
        if img_tag and img_tag.get("src"):
            src = img_tag["src"]
            absolute_src = "https://www.ncbi.nlm.nih.gov" + src if src.startswith("/") else src
            ext = os.path.splitext(absolute_src)[1] or ".png"
            fname = f"{pmcid}_{tid}{ext}"
            (out_dir / fname).write_bytes(
                requests.get(absolute_src, headers=HEADERS, timeout=15).content
            )
            print(f"🖼️   saved {fname} (probed)")

    return saved_count


# --------------------------------------------------------------------------- #
#                           Public‑facing helpers                             #
# --------------------------------------------------------------------------- #
def save_pmc_tables(pmcid: str, out_dir: Path):
    """
    Save **all** tables for a single article into `out_dir`.

    - Structured .txt tables    →  out_dir / RAW_DIR_NAME  (pipe‑delimited)
    - Images (.png/.jpg)        →  out_dir /
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = out_dir / RAW_DIR_NAME
    raw_dir.mkdir(exist_ok=True)

    structured_tables = 0

    # 1) XML
    xml_text = _xml_or_none(pmcid)
    if xml_text:
        for tid, txt in _tables_from_xml(xml_text):
            (raw_dir / f"{pmcid}_{tid}.txt").write_text(txt, encoding="utf-8")
            structured_tables += 1

    # 2) inline <div class="table-wrap">
    for kind, tid, payload in _classic_inline_tables(pmcid):
        if kind == "TXT":
            (raw_dir / f"{pmcid}_{tid}.txt").write_text(payload, encoding="utf-8")
            structured_tables += 1
        else:
            ext = os.path.splitext(payload)[1] or ".png"
            (out_dir / f"{pmcid}_{tid}{ext}").write_bytes(
                requests.get(payload, headers=HEADERS, timeout=15).content
            )

    # 3) linked /table/ pages
    if structured_tables == 0:  # avoid duplicate work if already found some
        for link in _html_table_links(pmcid):
            tid, txt = _table_to_csv(link)
            if txt:
                (raw_dir / f"{pmcid}_{tid}.txt").write_text(txt, encoding="utf-8")
                structured_tables += 1
            else:
                _attachment_image(link, out_dir, pmcid)

    # 4) PMC OA plain-text TSVs
    if structured_tables == 0:
        for tid, txt in _oa_tsv_tables(pmcid):
            (raw_dir / f"{pmcid}_{tid}.txt").write_text(txt, encoding="utf-8")
            structured_tables += 1

    # 5) Blind T1/T2/… probe
    if structured_tables == 0:
        structured_tables += _probe_T_pages(pmcid, out_dir, raw_dir)

    # final report
    if structured_tables == 0:
        print(f"⚠️  {pmcid}: no structured tables found.")
    else:
        print(f"✅  {pmcid}: saved {structured_tables} structured table(s).")


def batch_from_csv(csv_path: str, out_dir: str):
    """
    Iterate through a CSV with a single column 'pmcid' and download tables
    for every ID.  A 0.34 s pause is added to keep well within the NCBI
    courtesy policy.
    """
    with open(csv_path, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            pmcid = row.get("pmcid", "").strip()
            if pmcid:
                save_pmc_tables(pmcid, Path(out_dir))
                time.sleep(0.34)  # polite pause


def collect_slr_to_csv(paper_storage_folder: str, output_csv: str):
    """
    Scan a folder of JSON metadata files (each file contains one record with
    at least a `pmcid` and `type`).  Build a CSV of PMCIDs whose `type`
    starts with 'slr_'.
    """
    import json as _json

    pmc_rows: list[dict[str, str]] = []

    for fname in os.listdir(paper_storage_folder):
        if not fname.endswith(".json"):
            continue

        full_path = os.path.join(paper_storage_folder, fname)
        try:
            meta = _json.load(open(full_path, encoding="utf-8"))
        except Exception:
            continue

        if not meta.get("type", "").lower().startswith("slr_"):
            continue

        pmcid = meta.get("pmcid", "").strip()
        if pmcid:
            pmc_rows.append({"pmcid": pmcid})

    if not pmc_rows:
        print("⚠️  No SLR JSON files found.")
        return

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["pmcid"])
        writer.writeheader()
        writer.writerows(pmc_rows)

    print(f"✅  SLR list written → {output_csv}")
