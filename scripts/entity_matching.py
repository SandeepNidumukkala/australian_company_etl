import pandas as pd
import sqlalchemy
import requests
from sqlalchemy import create_engine
from rapidfuzz import fuzz, process
import re
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def clean_string(s):
    if not s: return ""
    s = s.lower().strip()
    s = re.sub(r"\bpty\s*\.?\s*ltd\b", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s

def block_dataframe(df, column, block_len=3):
    df['block'] = df[column].apply(lambda x: x[:block_len] if x else "zzz")
    return df

def call_hf_llm(record1, record2, api_key=None):
    # Easily swappable to any open-source LLM published on Hugging Face Inference API
    if not api_key:
        logger.warning("No Hugging Face API key; returning RapidFuzz score as fallback")
        return 80, "No API key; fallback to RapidFuzz"
    # Use Mistral-7B-Instruct or another open-access Hugging Face model
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    prompt = (
        f"Given two company records:\n"
        f"Record 1: {record1}\n"
        f"Record 2: {record2}\n\n"
        "Are these likely the same entity? Respond only with JSON like: "
        '{"confidence": 95, "reasoning": "Company names are nearly identical."}'
    )
    payload = {"inputs": prompt}
    try:
        response = requests.post(
            "https://api-inference.huggingface.co/models/mistralai/Mistral-7B-Instruct-v0.2",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        output = response.json()
        # Parse output from possible text to JSON (be robust)
        raw = output[0]["generated_text"] if isinstance(output, list) and "generated_text" in output[0] else ""
        try:
            start = raw.find("{")
            end = raw.rfind("}") + 1
            json_part = raw[start:end]
            info = eval(json_part, {}, {})  # Use json.loads(json_part) for strict JSON
            return info.get("confidence", 80), info.get("reasoning", "No rationale")
        except Exception as e:
            logger.warning(f"Failed parsing HF LLM response: {e}, [{raw}]")
            return 80, f"Failed to parse LLM: {raw[:100]}"
    except Exception as e:
        logger.warning(f"Hugging Face LLM error: {e}")
        return 80, f"HF call failed: {e}"

def entity_matching(api_key=None):
    engine = create_engine('postgresql+psycopg2://etl_user:secure_password@localhost:5432/company_db')
    cc_df = pd.read_sql('SELECT id, company_name, industry, url FROM raw_common_crawl', engine)
    abr_df = pd.read_sql('SELECT id, abn, entity_name, address, entity_type, entity_status, postcode, state, start_date FROM raw_abr', engine)
    logger.info(f"Loaded {len(cc_df)} CommonCrawl and {len(abr_df)} ABR records")
    # Clean & block
    cc_df['std_company_name'] = cc_df['company_name'].apply(clean_string)
    abr_df['std_entity_name'] = abr_df['entity_name'].apply(clean_string)
    cc_df = block_dataframe(cc_df, 'std_company_name')
    abr_df = block_dataframe(abr_df, 'std_entity_name')
    matches, unified_records = [], []
    for block in set(cc_df['block']).intersection(set(abr_df['block'])):
        cc_block = cc_df[cc_df['block'] == block]
        abr_block = abr_df[abr_df['block'] == block]
        for _, cc_row in cc_block.iterrows():
            candidates = process.extract(
                cc_row['std_company_name'], list(abr_block['std_entity_name']),
                scorer=fuzz.token_sort_ratio, limit=3)
            for (match_name, score, idx) in candidates:
                if score > 87:  # likely match
                    abr_row = abr_block.iloc[idx]
                    record1 = {"name": cc_row['company_name'], "industry": cc_row['industry'], "url": cc_row['url']}
                    record2 = {
                        "name": abr_row['entity_name'], "address": abr_row['address'],
                        "abn": abr_row['abn'], "entity_type": abr_row['entity_type'],
                        "entity_status": abr_row['entity_status'], "postcode": abr_row['postcode'], "state": abr_row['state']
                    }
                    # Use Hugging Face LLM for final check only on candidates above threshold
                    confidence, reasoning = call_hf_llm(record1, record2, api_key)
                    if confidence >= 90:
                        unified_records.append({
                            "abn": abr_row["abn"], "company_name": abr_row["entity_name"],
                            "entity_type": abr_row["entity_type"], "entity_status": abr_row["entity_status"],
                            "address": abr_row["address"], "postcode": abr_row["postcode"], "state": abr_row["state"],
                            "start_date": abr_row["start_date"], "industry": cc_row["industry"], "website_url": cc_row["url"]
                        })
    if unified_records:
        pd.DataFrame(unified_records).drop_duplicates(subset=["abn"]).to_sql(
            "unified_companies", engine, if_exists="append", index=False, chunksize=5000, method="multi"
        )
        logger.info(f"Inserted {len(unified_records)} matched companies.")

if __name__ == '__main__':
    hf_api_key = os.getenv("HUGGINGFACE_API_KEY")  # Place your token in env or hardcode if testing
    entity_matching(hf_api_key)
