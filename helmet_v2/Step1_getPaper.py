import os
from utils.step1_getArticles import step1_getPubmed

if __name__ == "__main__":
    # Define the project root as the directory containing this file.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Set the path for query instruction files inside DataLake/input_papers
    instructions_file_bim = os.path.join(BASE_DIR, "DataLake", "bim.txt")
    instructions_file_cem = os.path.join(BASE_DIR, "DataLake", "cem.txt")
    instructions_file_slr_bim = os.path.join(BASE_DIR, "DataLake", "slr_bim.txt")
    instructions_file_slr_cem = os.path.join(BASE_DIR, "DataLake", "slr_cem.txt")
    
    # Choose the query file you want to run.
    # For example, to run the SLR_CEM query:
    step1_getPubmed(instructions_file_slr_cem)
