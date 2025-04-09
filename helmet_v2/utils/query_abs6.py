import os
from llama_parse import LlamaParse
from typing import List
from pathlib import Path
from dotenv import load_dotenv
import json
import re
from llama_index.core import Settings
from llama_index.llms.openai import OpenAI
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_parse import LlamaParse
from llama_index.core.schema import TextNode
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core.indices.vector_store.base import VectorStoreIndex
from llama_index.core.query_engine import CustomQueryEngine
from llama_index.core.response_synthesizers import TreeSummarize, BaseSynthesizer
from llama_index.core.prompts import ChatPromptTemplate, ChatMessage
from llama_index.core.llms import LLM
from llama_index.core.async_utils import run_jobs, asyncio_run
from typing import List, Optional
from pydantic import BaseModel, Field
import pickle
import re
from pathlib import Path
from dotenv import load_dotenv
from llama_index.core.vector_stores.types import (
    VectorStoreInfo,
    VectorStoreQuerySpec,
    MetadataInfo,
    MetadataFilters,
    FilterCondition,
)
from llama_index.core.schema import NodeWithScore
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core.indices.vector_store.base import VectorStoreIndex
import nest_asyncio
from asyncio import run
from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.evaluation import DatasetGenerator
import csv
import openai

# Load environment variables
load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
openai.api_key = os.getenv("OPENAI_API_KEY")

# Initialize the LlamaParse API
parser = LlamaParse(
    api_key="llx-TIkKh89yEPGjm8UntRjDBWmd39Or5a6kT4I8wN5OpFJdLT3E",
    result_type="markdown"
)

def process_papers(data_dir: str, papers: List[str]):
    """
    Processes a list of PDF files, extracting metadata and content.

    Args:
        data_dir (str): Directory containing PDF files.
        papers (List[str]): List of PDF file names.

    Returns:
        dict: A dictionary with extracted metadata for each paper.
    """
    paper_dicts = {}
    for paper_path in papers:
        paper_base = Path(paper_path).stem
        full_paper_path = str(Path(data_dir) / paper_path)
        md_json_objs = parser.get_json_result(full_paper_path)
        json_dicts = md_json_objs[0]["pages"]
        paper_dicts[paper_path] = {
            "paper_path": full_paper_path,
            "json_dicts": json_dicts,
        }
    return paper_dicts

def get_text_nodes(json_dicts, paper_path):
    """
    Converts parsed JSON data into a list of TextNode objects.

    Args:
        json_dicts (list): Parsed JSON data for a paper.
        paper_path (str): Path to the paper.

    Returns:
        list: List of TextNode objects.
    """
    nodes = []
    for idx, md_text in enumerate([d["md"] for d in json_dicts]):
        chunk_metadata = {
            "page_num": idx + 1,
            "paper_path": paper_path,
        }
        nodes.append({"text": md_text, "metadata": chunk_metadata})
    return nodes

def create_abstract_nodes(json_folder, output_file):
    """
    Reads all JSON files in the specified folder and creates a single text file containing all nodes.

    Args:
        json_folder (str): Path to the folder containing JSON files with extracted abstracts.
        output_file (str): Path to the output file where nodes will be saved.
    """
    nodes = []
    
    # Get all JSON files in the folder
    json_files = [f for f in os.listdir(json_folder) if f.endswith('.json')]
    print(f"Found JSON files: {json_files}")

    for json_file in json_files:
        file_path = os.path.join(json_folder, json_file)

        with open(file_path, 'r', encoding='utf-8') as file:
            abstract_data = json.load(file)

        # Extract paper path (stem of the file name)
        for paper_key, abstracts in abstract_data.items():
            for idx, abstract in enumerate(abstracts):
                if abstract.get("abstract_exist", "no").lower() == "yes":
                    text = abstract.get("abstract_text", "").strip()
                    page_num = abstract.get("abstract_page", idx + 1)

                    if text:  # Only process if the abstract text exists
                        node = {
                            "text": text,
                            "metadata": {
                                "paper_path": Path(paper_key).stem,
                                "page_num": page_num,
                            },
                        }
                        nodes.append(node)

    # Write all nodes to a single text file
    with open(output_file, 'w', encoding='utf-8') as output:
        for node in nodes:
            output.write(json.dumps(node) + "\n")

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

def create_sentence_nodes(original_nodes_file, sentence_nodes_file, base_nodes_file):
    """
    Creates sentence-level nodes from original nodes.

    Args:
        original_nodes_file (str): Path to the file containing original nodes.
        sentence_nodes_file (str): Path to the output file where sentence-level nodes will be saved.
        base_nodes_file (str): Path to the output file where base nodes will be saved.
    """
    # Read original nodes
    with open(original_nodes_file, 'r', encoding='utf-8') as file:
        original_nodes = [json.loads(line) for line in file]

    # Split text into sentences
    sentence_nodes = []
    for original_node in original_nodes:
        sentences = split_text_into_sentences(original_node["text"])
        for sentence in sentences:
            sentence_node = {
                "text": sentence.strip(),
                "metadata": original_node["metadata"],
            }
            sentence_nodes.append(sentence_node)

    # Save sentence-level nodes to a file
    with open(sentence_nodes_file, 'w', encoding='utf-8') as output:
        for node in sentence_nodes:
            output.write(json.dumps(node) + "\n")

def filter_sentence_nodes(sentence_nodes_file, filter_paper_path, output_csv):
    """
    Filters sentence nodes based on metadata and writes results to a CSV file.

    Args:
        sentence_nodes_file (str): Path to the file containing sentence nodes.
        filter_paper_path (str): The paper_path to filter by.
        output_csv (str): Path to the output CSV file.
    """
    filtered_nodes = []

    # Read sentence nodes
    with open(sentence_nodes_file, 'r', encoding='utf-8') as file:
        for line in file:
            node = json.loads(line)
            if filter_paper_path == "all" or node["metadata"].get("paper_path") == filter_paper_path:
                filtered_nodes.append(node)

    # Write filtered nodes to a CSV file
    with open(output_csv, 'w', encoding='utf-8', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["text", "paper_path", "page_num"])

        for node in filtered_nodes:
            csvwriter.writerow([
                node["text"],
                node["metadata"].get("paper_path"),
                node["metadata"].get("page_num")
            ])

def generate_questions_from_text(input_csv, json_output, output_csv):
    """
    Generates questions for each text in the input CSV using DatasetGenerator, processes each node individually,
    and writes the results to an output CSV.

    Args:
        input_csv (str): Path to the input CSV file containing text.
        json_output (str): Path to save generated dataset as a JSON file.
        output_csv (str): Path to the output CSV file with generated questions.
    """
    rows = []

    # Read input CSV
    with open(input_csv, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            rows.append(row)

    # Prepare nodes for DatasetGenerator
    base_nodes = [
        Document(
            text=row["text"],
            metadata={"paper_path": row["paper_path"], "page_num": row["page_num"]}
        )
        for row in rows
    ]

    # Initialize variables to store queries and responses
    all_queries = []
    all_responses = []

    # Process each base node individually
    # Define custom question generation query
    custom_question_gen_query = (
        "You are a Teacher/Professor in health economics. Your task is to set up "
        "{num_questions_per_chunk} questions about the context information that can be answered by it. "
        "Always bring the (paper_path) inside the question."
        "Restrict the questions to the context information provided using the example below "
        "Example 1: Context information is 'This study evaluated the impact of the introduction of brodalumab on the "
        "pharmacy budget on US commercial health plans. paper_path=Feldman et al 2018'; query to be generated: 'What did the study by Feldman et al. 2018 evaluate?'."
    )

    for i, node in enumerate(base_nodes):
        dataset_generator = DatasetGenerator(
            [node],  # Send one node at a time
            llm=OpenAI(model="gpt-3.5-turbo"),
            show_progress=True,
            num_questions_per_chunk=1,  # Generate 1 question per text
            question_gen_query=custom_question_gen_query
        )

        # Generate dataset for the current node
        eval_dataset = asyncio_run(dataset_generator.agenerate_dataset_from_nodes(num=1))
        eval_dataset.save_json(json_output)  # Save intermediate results (optional)

        # Process JSON for the current node
        with open(json_output, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)

        queries = list(data.get("queries", {}).values())
        responses = list(data.get("responses", {}).values())

        # Append the first query and response to the overall list
        all_queries.append(queries[0] if queries else "No query available")
        all_responses.append(responses[0] if responses else "No response available")

    # Add queries and responses to rows
    for i, row in enumerate(rows):
        row["queries"] = all_queries[i]
        row["responses"] = all_responses[i]

    # Write to output CSV
    with open(output_csv, 'w', encoding='utf-8', newline='') as csvfile:
        csvwriter = csv.DictWriter(csvfile, fieldnames=["text", "paper_path", "page_num", "queries", "responses"])
        csvwriter.writeheader()
        csvwriter.writerows(rows)

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    json_folder = os.path.join(BASE_DIR, "files", "single_papers", "abstract", "json")
    original_nodes_file = os.path.join(BASE_DIR, "files", "single_papers", "abstract", "all_nodes.txt")
    sentence_nodes_file = os.path.join(BASE_DIR, "files", "single_papers", "abstract", "sentence_nodes.txt")
    base_nodes_file = os.path.join(BASE_DIR, "files", "single_papers", "abstract", "base_nodes.txt")

    create_abstract_nodes(json_folder, original_nodes_file)
    create_sentence_nodes(original_nodes_file, sentence_nodes_file, base_nodes_file)

    # Example filtering
    filter_paper_path = "all"
    filtered_csv = os.path.join(BASE_DIR, "files", "single_papers", "abstract", "all_abst_sentences.csv")
    filter_sentence_nodes(sentence_nodes_file, filter_paper_path, filtered_csv)

    # Generate questions
    json_output = os.path.join(BASE_DIR, "files", "single_papers", "abstract", "db_abstract.json")
    questions_csv = os.path.join(BASE_DIR, "files", "single_papers", "abstract", "db_abst.csv")
    generate_questions_from_text(filtered_csv, json_output, questions_csv)
