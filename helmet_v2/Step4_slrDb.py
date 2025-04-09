import os
import pandas as pd
import logging
import openai
import difflib
from dotenv import load_dotenv

load_dotenv()
openai.api_key = "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"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_llm_answer(question, model="gpt-3.5-turbo"):
    """
    Query an LLM with a given question using the ChatCompletion API.
    """
    try:
        if model not in ["gpt-3.5-turbo", "gpt-3.5-turbo-0125"]:
            logging.warning(f"Skipping model {model} (likely no access).")
            return "No access to this model."

        response = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant specialized in health economics."},
                {"role": "user", "content": question}
            ],
            temperature=0.3,
            max_tokens=150,
        )
        return response['choices'][0]['message']['content'].strip()
    except openai.error.RateLimitError:
        logging.error(f"Rate limit exceeded for {model}.")
        return "Quota exceeded."
    except openai.error.AuthenticationError:
        logging.error(f"Authentication failed for {model}. Check your API key.")
        return "Authentication error."
    except Exception as e:
        logging.error(f"Error querying {model}: {e}")
        return "Error"


def compute_similarity(answer1, answer2):
    """
    Compute a similarity score between two answers.
    """
    return difflib.SequenceMatcher(None, answer1, answer2).ratio()

def evaluate_llm_answers(query_csv, evaluation_csv):
    """
    For each question in the query CSV, get answers from GPT-3.5-turbo and GPT-4,
    compute a similarity score, and save the results.
    """
    df = pd.read_csv(query_csv)
    results = []
    
    for idx, row in df.iterrows():
        question = row["question"]
        pmcid = row["pmcid"]
        logging.info(f"Evaluating question {idx+1}/{len(df)} for PMCID {pmcid}")
        
        answer_gpt35 = get_llm_answer(question, model="gpt-3.5-turbo")
    # answer_gpt4 = get_llm_answer(question, model="gpt-4")  # ← Comment this out if no access
        answer_gpt4 = "Not available"

        similarity = compute_similarity(answer_gpt35, answer_gpt4)
        
        results.append({
            "pmcid": pmcid,
            "question": question,
            "answer_gpt35": answer_gpt35,
            "answer_gpt4": answer_gpt4,
            "similarity_score": similarity
        })
    
    result_df = pd.DataFrame(results)
    result_df.to_csv(evaluation_csv, index=False)
    logging.info(f"Evaluation results saved to {evaluation_csv}")
    return result_df

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    QUERY_CSV = os.path.join(BASE_DIR, "output_db", "query_db", "query_db.csv")
    EVALUATION_CSV = os.path.join(BASE_DIR, "output_db", "evaluation", "evaluation_results.csv")
    os.makedirs(os.path.dirname(EVALUATION_CSV), exist_ok=True)
    
    eval_df = evaluate_llm_answers(QUERY_CSV, EVALUATION_CSV)
    print(eval_df.describe())
