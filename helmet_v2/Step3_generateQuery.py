import os
from utils.step3_generate_query import generate_questions_from_abstract

if __name__ == "__main__":
    # Define the project root as the directory containing this file.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Path to the index database CSV (created in Step2)
    INDEX_FOLDER = os.path.join(BASE_DIR, "output_db", "index_db")
    index_db = os.path.join(INDEX_FOLDER, "index_db.csv")
    
    # Path for the query database CSV (will be created/updated in Step3)
    QUERY_FOLDER = os.path.join(BASE_DIR, "output_db", "query_db")
    os.makedirs(QUERY_FOLDER, exist_ok=True)
    query_db = os.path.join(QUERY_FOLDER, "query_db.csv")
    
    # Set the type filter as needed. For example, if you want to process BIM papers:
    # generate_questions_from_abstract(index_db, query_db, type_filter="bim")
    # Or if you want to process SLR_CEM papers:
    generate_questions_from_abstract(index_db, query_db, type_filter="slr_cem")
