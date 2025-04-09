import os
from utils.step2_indexDB import process_json_files

if __name__ == "__main__":
    # Define the project root as the directory containing this file.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # JSON files from Step 1 are stored in output_db/paper_storage/
    INPUT_FOLDER = os.path.join(BASE_DIR, "output_db", "paper_storage")
    
    # Output CSV (index database) will be stored in output_db/index_db/
    OUTPUT_FOLDER = os.path.join(BASE_DIR, "output_db", "index_db")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    OUTPUT_CSV_PATH = os.path.join(OUTPUT_FOLDER, "index_db.csv")
    
    # Folders for full text (redacted or SLR) are subfolders of paper_storage
    REDACTED_FOLDER = os.path.join(INPUT_FOLDER, "redacted_single_paper")
    SLR_FOLDER = os.path.join(INPUT_FOLDER, "slr_paper")
    
    # Process JSON files to create or update the index database
    process_json_files(INPUT_FOLDER, OUTPUT_CSV_PATH, REDACTED_FOLDER, SLR_FOLDER)
