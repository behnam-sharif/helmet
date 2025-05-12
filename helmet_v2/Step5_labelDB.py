import os
import csv
from utils.step5_labeling import extract_titles_and_paragraphs, generate_label_rows

INPUT_DIR = 'helmet_v2/output_db/paper_storage/redacted_single_paper'
OUTPUT_FILE = 'helmet_v2/output_db/label_db/label.csv'

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

rows = []

for filename in os.listdir(INPUT_DIR):
    if filename.endswith('_full_text.xml'):
        pubmed_id = filename.split('_')[0]
        xml_path = os.path.join(INPUT_DIR, filename)

        try:
            titles, paragraphs = extract_titles_and_paragraphs(xml_path)
            paper_rows = generate_label_rows(pubmed_id, titles, paragraphs, num_rows=2)
            rows.extend(paper_rows)
        except Exception as e:
            print(f"Error processing {filename}: {e}")

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['pubmed_id', 'section_choices', 'question', 'answer'])
    writer.writeheader()
    writer.writerows(rows)



print(f"Saved {len(rows)} rows to {OUTPUT_FILE}")
