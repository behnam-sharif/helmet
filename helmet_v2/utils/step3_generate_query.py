import os
import csv
import pandas as pd
from dotenv import load_dotenv
from llama_index.core import Document
from llama_index.core.evaluation import DatasetGenerator
from llama_index.llms.openai import OpenAI
from llama_index.core.async_utils import asyncio_run
import hashlib

# Load environment variables
load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

def split_text_into_sentences(doc_text):
    """
    Splits a document's text into sentences based on "." or "\n\n".
    
    Args:
        doc_text (str): The document text to split.
    
    Returns:
        List[str]: A list of sentences.
    """
    import re
    return re.split(r"(?<=\.)\s|\n\n", doc_text)

def compute_hash(text):
    """
    Computes an MD5 hash for the given text.
    
    Args:
        text (str): The text for which to compute the hash.
    
    Returns:
        str: The MD5 hash.
    """
    if not isinstance(text, str) or not text.strip():
        return ""
    return hashlib.md5(text.strip().encode('utf-8')).hexdigest()

def generate_questions_from_abstract(index_db, query_db, type_filter=None):
    """
    Generates questions from abstracts in the index database with optional filtering by type.
    A paper is considered "new" if its PMCID is not in the query database or if its abstract text
    has changed (based on an MD5 hash).
    
    Args:
        index_db (str): Path to the input CSV file containing the index database.
        query_db (str): Path to the output CSV file for storing queries and responses.
        type_filter (str, optional): Filter abstracts by type (e.g., 'bim'). If None, process all types.
    """
    # Load the index database
    if not os.path.exists(index_db):
        print(f"No index database found at {index_db}")
        return

    index_df = pd.read_csv(index_db)
    if index_df.empty:
        print("Index database is empty. Nothing to process.")
        return

    # Compute an MD5 hash for each abstract
    index_df["abstract_hash"] = index_df["abstract"].apply(compute_hash)
    
    # Clean the "type" field to remove extra spaces and standardize case
    index_df["type"] = index_df["type"].astype(str).str.strip().str.lower()
    
    print("Total rows in index DB:", len(index_df))
    print("Unique types in index DB:", index_df["type"].unique())
    
    # If a filter is provided, clean it as well and apply
    if type_filter:
        type_filter = type_filter.strip().lower()
        index_df = index_df[index_df["type"] == type_filter]
        if index_df.empty:
            print("No matching rows after type filtering.")
            return

    # Load the query database if it exists; otherwise create an empty DataFrame
    if os.path.exists(query_db):
        query_df = pd.read_csv(query_db)
        if "abstract_hash" not in query_df.columns:
            query_df["abstract_hash"] = ""
    else:
        query_df = pd.DataFrame(columns=["pmcid", "first_author", "title", "year", "type",
                                         "sentence", "question", "answer", "abstract_hash"])
    
    # Build a dictionary mapping PMCID to stored abstract hash
    existing_hashes = dict(zip(query_df["pmcid"].astype(str), query_df["abstract_hash"]))
    
    # Determine which rows are new or whose abstract has changed
    def is_new_or_changed(row):
        pmcid_str = str(row["pmcid"])
        new_hash = row["abstract_hash"]
        old_hash = existing_hashes.get(pmcid_str, "")
        return (old_hash != new_hash)
    
    new_rows = index_df[index_df.apply(is_new_or_changed, axis=1)]
    
    if new_rows.empty:
        print("No new abstracts to process.")
        return
    
    print(f"Found {len(new_rows)} new or changed abstracts to process.")
    rows_to_process = new_rows.to_dict(orient="records")
    
    # Prepare Document nodes for question generation
    sentence_nodes = []
    for row in rows_to_process:
        abstract_text = row.get("abstract", "") or ""
        hashed = row["abstract_hash"]
        sentences = split_text_into_sentences(abstract_text)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
        for idx, sentence in enumerate(sentences):
            sentence_nodes.append(
                Document(
                    text=sentence,
                    metadata={
                        "pmcid": row["pmcid"],
                        "first_author": row["first_author"],
                        "title": row["title"],
                        "year": row["year"],
                        "type": row["type"],
                        "sentence_index": idx + 1,
                        "abstract_hash": hashed,
                    }
                )
            )
    
    # Custom question generation query (same as original comments)
    custom_question_gen_query = (
        "Based on the following abstract sentence, generate one question that can be answered using the sentence. "
        "Include the (pmcid) in the question. "
        "Restrict the questions to the context information provided using the examples below: "
        "Example 1: Context is 'This study evaluated the impact of the introduction of brodalumab on the "
        "pharmacy budget on US commercial health plans. pmcid=2298'; question: 'What did the study evaluate (pmcid=2298)?'. "
        "Example 2: Context is 'Methods: A cross-indication budget impact model was designed to estimate the effects of adding secukinumab in the Italian market from the NHS perspective over 3 years. pmcid=3445'; "
        "question: 'What was the perspective of the analysis (pmcid=3445)?'."
    )
    
    all_queries = []
    all_responses = []
    
    # Generate questions using DatasetGenerator for each Document node
    for i, node in enumerate(sentence_nodes):
        try:
            print(f"Processing node {i + 1}/{len(sentence_nodes)}: {node.text[:80]}...")
            dataset_generator = DatasetGenerator(
                [node],
                llm=OpenAI(model="gpt-3.5-turbo"),
                show_progress=True,
                num_questions_per_chunk=1,
                question_gen_query=custom_question_gen_query
            )
            eval_dataset = asyncio_run(dataset_generator.agenerate_dataset_from_nodes(num=1))
            queries = list(eval_dataset.queries.values())
            responses = list(eval_dataset.responses.values())
            q = queries[0] if queries else "No query"
            r = responses[0] if responses else "No response"
            all_queries.append(q)
            all_responses.append(r)
        except Exception as e:
            print(f"Error generating questions for node {i+1}: {e}")
            all_queries.append("Error")
            all_responses.append("Error")
    
    # Build new rows for the query database
    new_data = []
    for idx, node in enumerate(sentence_nodes):
        new_data.append({
            "pmcid": node.metadata["pmcid"],
            "first_author": node.metadata["first_author"],
            "title": node.metadata["title"],
            "year": node.metadata["year"],
            "type": node.metadata["type"],
            "sentence": node.text,
            "question": all_queries[idx],
            "answer": all_responses[idx],
            "abstract_hash": node.metadata["abstract_hash"],
        })
    
    # Append new rows to the existing query database and save
    query_df = pd.concat([query_df, pd.DataFrame(new_data)], ignore_index=True)
    query_df.to_csv(query_db, index=False)
    print(f"Query database updated and saved to: {query_db}")
