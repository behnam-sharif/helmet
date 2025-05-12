import os
import random
import xml.etree.ElementTree as ET
from llama_index.llms.openai import OpenAI
import json
from dotenv import load_dotenv
from llama_index.core.schema import TextNode
from llama_index.core.evaluation import DatasetGenerator
from llama_index.core.async_utils import asyncio_run
from nltk.tokenize import sent_tokenize
import nltk
from llama_index.core.prompts import PromptTemplate

nltk.download('punkt')
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


def is_valid_sentence(sentence, section, llm):
    prompt_template_str = (
        "You are an expert agent. Your job is to decide whether the given sentence is suitable for classification.\n"
        "Return 'yes' only if ALL of the following are true:\n"
        "- It is a complete, well-formed sentence that has a verb and subject and has at least 5 words. For example, it can not be 'Sojourn time' or 'direct cost' or '100% reduction'.\n"
        "- It is not a citation, header, or fragment.\n"
        "- It is from sections related to Introduction, Methods, Results or Conclusion or those related to modleing (Cost, input paramters, perspective, CEM Results, Analysis,etc.) and not from sections that are not related to core scientific description, for example  it can not be from References or Supplementary material or 'M' or 'D' or Author contributions,Abbreviations,Ethical Declaration,Conflicts of Interest,Data Availability Statement,Acknowledgments,Code availability,Ethics approval,availability of data and material.\n\n"
        "Sentence: \"{sentence}\"\n"
        "Section: \"{section}\"\n\n"
        "Answer only: yes or no"
    )

    prompt = PromptTemplate(template=prompt_template_str)

    try:
        response = llm.predict(prompt, sentence=sentence, section=section).strip().lower()
        return response == 'yes'
    except Exception as e:
        print(f"LLM error while validating sentence: {e}")
        return False

def generate_label_rows(pubmed_id, titles, paragraphs, num_rows=2):
    # Step 1: Define excluded section keywords
    EXCLUDED_SECTIONS = [
        "references", "supplementary", "author contributions", "abbreviations", "ethical declaration",
        "conflicts of interest", "data availability statement", "acknowledgments", "acknowledgements",
        "code availability", "ethics approval", "availability of data and materials", "funding", "declarations"
    ]
    EXCLUDED_SHORT_CODES = ["D", "M"]

    # Step 2: Filter titles
    titles = [
        t for t in titles
        if not any(ex in t.lower() for ex in EXCLUDED_SECTIONS)
        and t.strip().upper() not in EXCLUDED_SHORT_CODES
    ]
    if len(titles) < 5:
        return []

    # Step 3: Initialize LLM and shuffle paragraphs
    llm = OpenAI(model="gpt-3.5-turbo")
    random.shuffle(paragraphs)

    rows = []
    used_paragraphs = set()

    for para_text, section in paragraphs:
        section_clean = section.strip().lower()

        # Step 4: Skip excluded paragraph sections
        if (
            any(ex in section_clean for ex in EXCLUDED_SECTIONS)
            or section.strip().upper() in EXCLUDED_SHORT_CODES
        ):
            continue

        if para_text in used_paragraphs:
            continue

        # Step 5: Tokenize and pick one sentence
        sentences = sent_tokenize(para_text)
        if not sentences:
            continue
        sentence = random.choice(sentences)

        # Step 6: Validate sentence with LLM
        if not is_valid_sentence(sentence, section, llm):
            continue

        # Step 7: Select wrong options and build question
        wrong_sections = [t for t in titles if t != section]
        if len(wrong_sections) < 4:
            continue

        section_choices = random.sample(wrong_sections, 4)
        section_choices.append(section)
        random.shuffle(section_choices)

        row = {
            'pubmed_id': pubmed_id,
            'section_choices': '; '.join(section_choices),
            'question': f'Which section does this sentence belong to: \"{sentence}\"',
            'answer': section
        }
        rows.append(row)
        used_paragraphs.add(para_text)

        if len(rows) >= num_rows:
            break

    return rows
