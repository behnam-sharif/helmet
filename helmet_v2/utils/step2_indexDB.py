import os
import json
import pandas as pd

def process_json_files(input_folder, output_csv, redacted_folder, slr_folder):
    """
    Process JSON files and update index database (CSV) without duplicates,
    keeping only the last occurrence of each PMCID (ignoring type differences).
    Only outputs the number of new articles added.
    """
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    # Load existing index DB
    if os.path.exists(output_csv):
        index_db = pd.read_csv(output_csv, dtype=str)
        index_db["pmcid"] = index_db["pmcid"].str.strip()
        existing_pmcids = set(index_db["pmcid"])
    else:
        index_db = pd.DataFrame(columns=["pmcid", "first_author", "title", "source", "year", "abstract", "type"])
        existing_pmcids = set()

    data_to_add = []

    for file_name in os.listdir(input_folder):
        if not file_name.endswith(".json"):
            continue

        file_path = os.path.join(input_folder, file_name)

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)

            pmcid = str(json_data.get("pmcid", "")).strip()

            if not pmcid:
                continue  # Skip files with missing PMCID

            if pmcid in existing_pmcids:
                continue  # Skip duplicates silently

            row = {
                "pmcid": pmcid,
                "first_author": json_data.get("first_author", ""),
                "title": json_data.get("title", ""),
                "source": json_data.get("source", ""),
                "year": json_data.get("year", ""),
                "abstract": json_data.get("abstract", ""),
                "type": json_data.get("type", "").strip().lower()
            }
            data_to_add.append(row)
            existing_pmcids.add(pmcid)

        except (json.JSONDecodeError, OSError) as e:
            print(f"Error reading {file_name}: {e}")

    # Combine and write
    if data_to_add:
        new_data_df = pd.DataFrame(data_to_add)
        combined_df = pd.concat([index_db, new_data_df], ignore_index=True)
        combined_df["pmcid"] = combined_df["pmcid"].str.strip()

        # Deduplicate by PMCID only — keep last
        combined_df = combined_df.drop_duplicates(subset="pmcid", keep="last")

        combined_df.to_csv(output_csv, index=False)
        print(f"{len(data_to_add)} new article(s) added.")
    else:
        print("No new articles added.")
