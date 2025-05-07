import os
import random
import xml.etree.ElementTree as ET
from llama_index.llms.openai import OpenAI
import json
from dotenv import load_dotenv
from llama_index.core.schema import TextNode
from llama_index.core.evaluation import DatasetGenerator
from llama_index.core.async_utils import asyncio_run


# Load environment variables
load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

def extract_titles_and_paragraphs(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    titles = []
    paragraphs = []
    current_title = None

    for elem in root.iter():
        if elem.tag.lower() == 'title':
            current_title = elem.text.strip() if elem.text else ''
            if current_title:
                titles.append(current_title)
        elif elem.tag.lower() == 'p':
            if elem.text and current_title:
                paragraphs.append((elem.text.strip(), current_title))

    return titles, paragraphs

def generate_label_rows(pubmed_id, titles, paragraphs, num_rows=1):
    # Step 1: Filter out bad section titles
    titles = [t for t in titles if "supplementary" not in t.lower() and "reference" not in t.lower()]
    if len(titles) < 5:
        return []

    # Step 2: Filter paragraphs using GPT (LlamaIndex DatasetGenerator method)
    #paragraphs = filter_sentences_with_llamaindex_datasetgen(paragraphs, pubmed_id)
    if len(paragraphs) < num_rows:
        return []

    # Step 3: Select random subset
    selected_paragraphs = random.sample(paragraphs, num_rows)
    rows = []

    for para_text, correct_section in selected_paragraphs:
        wrong_sections = [t for t in titles if t != correct_section]
        if len(wrong_sections) < 4:
            continue  # skip if not enough wrong options

        section_choices = random.sample(wrong_sections, 4)
        section_choices.append(correct_section)
        random.shuffle(section_choices)

        row = {
            'pubmed_id': pubmed_id,
            'section_choices': '; '.join(section_choices),
            'question': f'Which section does this sentence belong to: "{para_text}"',
            'answer': correct_section
        }
        rows.append(row)

    return rows




def convert_to_nodes(paragraphs, pubmed_id):
    nodes = []
    for text, section in paragraphs:
        node = TextNode(
            text=text,
            metadata={
                "section": section,
                "pmcid": pubmed_id
            }
        )
        nodes.append(node)
    return nodes

filter_instruction = (
    "You are an expert agent. Your job is to filter a list of sentences extracted from a scientific paper.\n"
    "Return only those that meet all of the following rules:\n"
    "- Sentence must be a complete, well-formed sentence.\n"
    "- Must NOT be a phrase, citation fragment, or header (e.g., 'Japanese guidelines', 'KDIGO').\n"
    "- Must NOT come from a 'References' or 'Supplementary' section.\n"
    "- Must contain only ONE sentence (not multiple sentences).\n"
    "Return a JSON list of objects like: {\"sentence\": ..., \"section\": ...}"
)



def filter_sentences_with_llamaindex_datasetgen(paragraphs, pubmed_id):
    llm = OpenAI(model="gpt-3.5-turbo")
    nodes = convert_to_nodes(paragraphs, pubmed_id)

    dataset_generator = DatasetGenerator(
        nodes,
        llm=llm,
        show_progress=True,
        question_gen_query=filter_instruction,
        num_questions_per_chunk=1
    )

    try:
        dataset = asyncio_run(dataset_generator.agenerate_dataset_from_nodes(num=len(nodes)))
        cleaned = []

        for idx, node in enumerate(nodes):
            answer = dataset.responses.get(node.node_id, "")
            if answer and isinstance(answer, str) and len(answer.split()) > 5:
                cleaned.append((answer.strip(), node.metadata["section"]))

        return cleaned
    except Exception as e:
        print(f"Error during LLM filtering: {e}")
        return []
