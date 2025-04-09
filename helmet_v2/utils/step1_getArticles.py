import os
import requests
import json
from xml.etree import ElementTree as ET
from dotenv import load_dotenv
from lxml import etree

def step1_getPubmed(instructions_file):
    """
    Fetch PMC articles and save details and full text based on instructions from a file.
    
    Args:
        instructions_file (str): Path to the instructions file (e.g., bim.txt, cem.txt, etc.).
        
    Output:
        Saves JSON files (paper details) and XML full-text files under output_db/paper_storage.
    """
    # Determine type from filename (e.g., bim, cem, slr_bim, slr_cem)
    instruction_type = os.path.splitext(os.path.basename(instructions_file))[0]
    
    # Define the project root (one level above the utils folder)
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Folder for storing paper details and full text
    PAPER_STORAGE_FOLDER = os.path.join(PROJECT_ROOT, "output_db", "paper_storage")
    os.makedirs(PAPER_STORAGE_FOLDER, exist_ok=True)
    
    # Load environment variables (make sure a .env file exists with your PUBMED_API_KEY)
    load_dotenv()
    os.environ["PUBMED_API_KEY"] = os.getenv("PUBMED_API_KEY")
    API_KEY = os.getenv("PUBMED_API_KEY")
    
    try:
        # Step 1: Read the query instructions from the file
        with open(instructions_file, 'r', encoding='utf-8') as file:
            query_instructions = file.read().strip()
        
        # Step 2: Fetch article PMCIDs from PMC
        pmcid_list = fetch_pmc_articles(query_instructions, API_KEY)
        
        # Step 3: For each PMCID, fetch details and full text
        for pmcid in pmcid_list:
            print(f"Fetching details for PMCID: {pmcid}")
            
            # Fetch article details and add the query type
            article_details = fetch_pmc_article_details(pmcid, API_KEY)
            article_details["type"] = instruction_type
            
            # Save details as JSON
            json_file = os.path.join(PAPER_STORAGE_FOLDER, f"{pmcid}.json")
            save_article_details(json_file, pmcid, article_details)
            print(f"Saved details for PMCID {pmcid} -> {json_file}")
            
            # Fetch full text
            full_text = fetch_pmc_full_text(pmcid, API_KEY)
            if full_text:
                # Determine subfolder based on type
                if instruction_type in ["bim", "cem"]:
                    full_text_folder = os.path.join(PAPER_STORAGE_FOLDER, "redacted_single_paper")
                    os.makedirs(full_text_folder, exist_ok=True)
                    # Optionally remove <abstract> from the XML
                    full_text = remove_abstract_from_xml(full_text)
                elif instruction_type in ["slr_bim", "slr_cem"]:
                    full_text_folder = os.path.join(PAPER_STORAGE_FOLDER, "slr_paper")
                    os.makedirs(full_text_folder, exist_ok=True)
                else:
                    print(f"Unknown type: {instruction_type}. Skipping full text storage.")
                    continue
                    
                full_text_file = os.path.join(full_text_folder, f"{pmcid}_full_text.xml")
                save_full_text(full_text_file, full_text)
                print(f"Saved full text for PMCID {pmcid} -> {full_text_file}")
            else:
                print(f"No full text available for PMCID {pmcid}")
                
        print("Completed fetching and saving article details/full text.")
        
    except Exception as e:
        print(f"An error occurred in step1_getPubmed: {e}")

def remove_abstract_from_xml(xml_content):
    """
    Removes the <abstract> tag and its content from the given XML content.
    """
    try:
        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.fromstring(xml_content, parser)
        for abstract in root.findall(".//abstract"):
            parent = abstract.getparent()
            if parent is not None:
                parent.remove(abstract)
        return etree.tostring(root, pretty_print=True, encoding="unicode")
    except etree.XMLSyntaxError as e:
        print(f"Error parsing XML content: {e}")
        return xml_content

# Supporting functions below

def fetch_pmc_articles(query, api_key, max_results=10):
    """Fetch articles from PMC using the Entrez API."""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        'db': 'pmc',
        'term': query,
        'retmode': 'json',
        'retmax': max_results,
        'api_key': api_key
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("esearchresult", {}).get("idlist", [])

def fetch_pmc_article_details(pmcid, api_key):
    """
    Fetch details including first author, title, source, year, and abstract.
    """
    esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    esummary_params = {
        'db': 'pmc',
        'id': pmcid,
        'retmode': 'json',
        'api_key': api_key
    }
    response = requests.get(esummary_url, params=esummary_params)
    response.raise_for_status()
    esummary_data = response.json()
    result = esummary_data.get("result", {}).get(pmcid, {})
    
    abstract_text = fetch_pmc_abstract(pmcid, api_key)
    
    return {
        "pmcid": pmcid,
        "first_author": (result.get("authors") or [{}])[0].get("name", ""),
        "title": result.get("title", ""),
        "source": result.get("source", ""),
        "year": (result.get("pubdate", "").split() or [""])[0],
        "abstract": abstract_text
    }

def fetch_pmc_abstract(pmcid, api_key):
    """Fetch abstract from PMC."""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        'db': 'pmc',
        'id': pmcid,
        'rettype': 'medline',
        'retmode': 'text',
        'api_key': api_key
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    abstract_lines = []
    capture = False
    for line in response.text.splitlines():
        if line.startswith("AB"):
            capture = True
            abstract_lines.append(line[3:].strip())
        elif capture:
            if line.startswith("  "):
                abstract_lines.append(line.strip())
            else:
                break
    return " ".join(abstract_lines).strip()

def fetch_pmc_full_text(pmcid, api_key):
    """Fetch full text from PMC."""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        'db': 'pmc',
        'id': pmcid,
        'rettype': 'full',
        'retmode': 'xml',
        'api_key': api_key
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    if response.text.strip():
        print(f"Successfully fetched full text for PMCID: {pmcid}")
        return response.text
    else:
        print(f"No full text available for PMCID: {pmcid}")
        return None

def save_article_details(file_path, pmcid, article_details):
    """Save article details as JSON."""
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(article_details, file, indent=4)

def save_full_text(file_path, full_text):
    """Save full text (XML) to a file."""
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(full_text)
