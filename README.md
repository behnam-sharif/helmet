# HELMET: Health Economics Language Model Evaluation Toolkit

**HELMET** is an open-source benchmark dataset designed to evaluate the performance of large language models (LLMs) in the context of **Health Economics and Outcomes Research (HEOR)**. HELMET provides standardized evaluation resources across three key HEOR tasks:

1. **Data Extraction**
2. **Evidence Synthesis**
3. **Labeling**

These tasks are curated to reflect real-world HEOR workflows, enabling both academic and industry researchers to systematically assess the reliability, accuracy, and relevance of LLMs in this specialized domain.

---


<pre> ## 📁 HELMET Directory Structure ```
  HELMET/ 
  ├── datalake/ # Instructions for PubMed API and temporary folder to fetch JSON from PubMed 
  ├── utils/ # Python utility functions used across all steps 
  ├── output_db/ # Stores generated files and databases from all pipeline stages 
  │ ├── paper_storage/ # JSON and redacted full-text papers 
  │ ├── index_db/ # Indexed paper metadata (pmcid, title, abstract, etc.) 
  │ ├── query_db/ # Data extraction queries │ ├── slr_db/ # Evidence synthesis (SLR) queries
  │ └── label_db/ # Labeling queries for contextual classification
  ├── step1_get_papers/ # Scripts to download papers from PubMed 
  ├── step2_index_metadata/ # Scripts to index paper metadata into index_db 
  ├── step3_generate_queries/ # Scripts to generate data extraction queries
  ├── step4_generate_slr/ # Scripts to generate evidence synthesis prompts 
  ├── step5_generate_labels/ # Scripts to generate context-aware labeling data 
  └── README.md # Repository documentation ``` </pre>
## 🔍 Tasks Overview

### 1. Data Extraction
- Extract structured elements (e.g., treatment name, cost, QALY, setting) from plain text such as HTA reports or publications.
- Each example includes a gold-standard JSON output and model-generated predictions for comparison.

### 2. Evidence Synthesis
- Input: Multiple abstracts or study summaries.
- Goal: Generate a coherent narrative summary (qualitative or quantitative) similar to those found in systematic reviews or value dossiers.

### 3. Labeling
- Assign context-aware labels (e.g., "clinical outcome", "economic input", "study design") to text snippets.
- Useful for RAG pipelines or domain-specific retrieval systems.

## Pipeline Steps
Step 1: Retrieve Papers

Use datalake/ to run PubMed API queries and save JSON metadata/full texts.

Step 2: Index Metadata

Add paper metadata (pmcid, title, abstract, etc.) into index_db.

Step 3: Generate Queries

Data extraction: Create structured query prompts and responses in query_db.

Systematic literature review: Generate SLR queries and results in slr_db.

Labeling: Generate labeling prompts and options in label_db.
