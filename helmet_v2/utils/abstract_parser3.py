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

# Load environment variables
load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

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

class AbstractOutput(BaseModel):
    """The metadata for the abstract section. Includes whether it exists, its page number, and its text."""

    abstract_exist: str = Field(
        ..., description="Flag indicating whether the abstract exists in the document text (yes/no)."
    )
    abstract_page: Optional[int] = Field(
        None, description="The page number where the abstract is located."
    )
    abstract_text: Optional[str] = Field(
        None, description="The text of the abstract section from the document text."
    )

class AbstractsOutput(BaseModel):
    """A list of extracted abstract information."""
    abstracts: List[AbstractOutput]

async def aget_abstract(
    doc_text: str, llm: Optional[LLM] = None
) -> List[AbstractOutput]:
    """Get extracted abstract metadata from a provided text."""
    system_prompt = """\
    You are an AI assistant tasked with finding the abstract section from the provided document which is a single page of a pdf from an academic paper.
    
    - Extract the following metadata:
          1. Whether the abstract exists in the document (yes/no).
          2. The page number where the abstract is located.
          3. The full text of the abstract section.
              - These are rules to follow to find out where the abstract starts:
                Rule #1. The abstract is always immidiately after the article title and authors names.
                Rule #2. Abstract always begin with a hashtag (#), and may have 'Abstract' or 'ABSTRACT'  or 'Summary' as a keyword after which the abstract starts.  If there is no 'Abstract' mentioned in the text after a hashtag (#) , it is possible that the abstract is still there, but may start with hashtag (#) and 'Objective' or 'Aim' or 'Background' or 'Introduction'.
              - These are rules to follow to find out where the abstract ends:
                Rule #1. An abstract always have "Results" as their keyword. The extracted text is NOT an abstract if it does not have the exact keywords of "Result" or "Results" or "RESULTS" inside the text.
                Rule #2. An abstract describes all of the following topics of a scientific research : "objective, methods, results and conclusions" . The extracted text is NOT an abstract if it only discusses one of these topics.
    - If no abstract exists, return `abstract_exist=no` with `abstract_page` and `abstract_text` set to None.
    - Ensure to extract only the text belonging to the abstract and exclude unrelated content.
    """
    llm = OpenAI(model="gpt-4o")

    chat_template = ChatPromptTemplate(
        [
            ChatMessage.from_str(system_prompt, "system"),
            ChatMessage.from_str("Document text: {doc_text}", "user"),
        ]
    )

    # Get prediction result
    result = await llm.astructured_predict(
        AbstractsOutput, chat_template, doc_text=doc_text
    )

    # Debugging output
    print("DEBUG: Result from llm.astructured_predict:", result)

    # Validate result and return abstracts
    if hasattr(result, 'abstracts'):
        return result.abstracts
    else:
        raise ValueError(f"Unexpected result format: {result}")

async def acreate_sections(text_nodes_dict):
    sections_dict = {}
    for paper_path, text_nodes in text_nodes_dict.items():
        abstract_dict = {}
        
        tasks = [aget_abstract(n.get("text", "")) for n in text_nodes]

        async_results = await run_jobs(tasks, workers=8, show_progress=True)
        all_abstracts = [s for r in async_results for s in r]

        abstract_dict[paper_path] = [a.dict() for a in all_abstracts]  # Convert to JSON serializable
    return abstract_dict

def extract_sections_from_folders(root_folder):
    """Extracts sections, including abstracts, from PDFs in the new folder structure."""
    papers_folder = os.path.join(root_folder, "single_papers", "pdf")
    output_folder = os.path.join(root_folder, "single_papers", "abstract", "json")

    if not os.path.exists(papers_folder):
        print(f"Papers folder does not exist: {papers_folder}")
        return

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    pdf_files = [f for f in os.listdir(papers_folder) if f.endswith('.pdf')]
    paper_dicts = process_papers(papers_folder, pdf_files)

    for paper_path, paper_data in paper_dicts.items():
        json_dicts = paper_data["json_dicts"]

        # Extract and save abstracts
        text_nodes = get_text_nodes(json_dicts, paper_data["paper_path"])
        text_nodes = text_nodes[:3]  # Only the first 3 nodes
        text_nodes_dict = {paper_path: text_nodes}
        abstract_dict = run(acreate_sections(text_nodes_dict))
        abstract_output_path = os.path.join(output_folder, f"{Path(paper_path).stem}_abstracts.json")
        with open(abstract_output_path, 'w', encoding='utf-8') as abstract_file:
            json.dump(abstract_dict, abstract_file, indent=4)

if __name__ == "__main__":
    # Use the project's relative folder "files" as the base for processing
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    root_folder = os.path.join(BASE_DIR, "files")
    extract_sections_from_folders(root_folder)
