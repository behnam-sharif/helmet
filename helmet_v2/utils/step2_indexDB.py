import os
import json
import pandas as pd

def process_json_files(input_folder, output_csv, redacted_folder, slr_folder):
    """
    Process JSON files in a folder and create or update an index database.
    
    Args:
        input_folder (str): Folder containing JSON files (from Step 1).
        output_csv (str): Path to save the resulting CSV file.
        redacted_folder (str): Folder for redacted full text.
        slr_folder (str): Folder for SLR full text.
    """
    # Ensure the output folder exists
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    # Load the existing index database if it exists
    if os.path.exists(output_csv):
        index_db = pd.read_csv(output_csv)
        existing_pmcids = set(index_db["pmcid"].astype(str))
    else:
        index_db = pd.DataFrame(columns=["pmcid", "first_author", "title", "source", "year", "abstract", "type"])
        existing_pmcids = set()
    
    data_to_add = []
    kept_full_texts = set()  # Track retained full-text PMCID
    
    # Iterate over all JSON files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(input_folder, file_name)
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    json_data = json.load(file)
                
                pmcid = json_data.get("pmcid", "")
                doc_type = json_data.get("type", "")
                
                # Duplicate check: if PMCID already exists, update only if the type differs
                if pmcid in existing_pmcids:
                    existing_record = index_db.loc[index_db["pmcid"] == pmcid]
                    if not existing_record.empty:
                        existing_type = existing_record["type"].iloc[0]
                        if existing_type.lower() != doc_type.lower():
                            print(f"PMCID {pmcid} exists with type '{existing_type}'. Updating to '{doc_type}'.")
                            index_db = index_db[index_db["pmcid"] != pmcid]
                            existing_pmcids.remove(pmcid)
                        else:
                            print(f"Duplicate found for PMCID {pmcid} with same type '{doc_type}'. Removing JSON.")
                            # Optionally, ensure one copy of the full text is retained
                            if pmcid not in kept_full_texts:
                                if doc_type in ["bim", "cem"]:
                                    full_text_file = os.path.join(redacted_folder, f"{pmcid}_full_text.xml")
                                elif doc_type in ["slr_bim", "slr_cem"]:
                                    full_text_file = os.path.join(slr_folder, f"{pmcid}_full_text.xml")
                                else:
                                    print(f"Unknown type: {doc_type}. Skipping full text.")
                                    continue
                                if os.path.exists(full_text_file):
                                    kept_full_texts.add(pmcid)
                                    print(f"Retained full text: {full_text_file}")
                            continue  # Skip this JSON file
                
                # Add new (or updated) record
                row = {
                    "pmcid": pmcid,
                    "first_author": json_data.get("first_author", ""),
                    "title": json_data.get("title", ""),
                    "source": json_data.get("source", ""),
                    "year": json_data.get("year", ""),
                    "abstract": json_data.get("abstract", ""),
                    "type": doc_type
                }
                data_to_add.append(row)
                
            except (json.JSONDecodeError, OSError) as e:
                print(f"Error reading {file_name}: {e}")
    
    # Append new data if available
    if data_to_add:
        new_data_df = pd.DataFrame(data_to_add)
        updated_index_db = pd.concat([index_db, new_data_df], ignore_index=True)
        updated_index_db.to_csv(output_csv, index=False)
        print(f"Updated CSV file created: {output_csv}")
    else:
        print("No new data to add to the index database.")
