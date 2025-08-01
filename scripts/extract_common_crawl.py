import gzip
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
import psycopg2
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_CONFIG = {
    "dbname": "company_db",
    "user": "etl_user",
    "password": "your_password",
    "host": "localhost",
    "port": "5432"
}

MAX_URL_LEN = 2048
MAX_COMPANY_LEN = 255
MAX_INDUSTRY_LEN = 1000
BATCH_SIZE = 1000

def truncate(text, max_length):
    if text:
        return text[:max_length]
    return None

def clean_company_name(name):
    import re
    if not name:
        return ""
    return re.sub(r"\bpty\s*\.?\s*ltd\b", "", name, flags=re.IGNORECASE).strip().lower()

def download_and_process_warc(url):
    try:
        logging.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        logging.info(f"Downloading and processing WARC: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        batch_records = []
        processed_count = 0
        with gzip.GzipFile(fileobj=response.raw) as gz:
            for record in ArchiveIterator(gz):
                if record.rec_type != "response":
                    continue
                target_url = record.rec_headers.get_header("WARC-Target-URI")
                if not target_url or ".au" not in target_url:
                    continue
                try:
                    payload = record.content_stream().read()
                    soup = BeautifulSoup(payload, "html.parser")
                    company_name = soup.title.string.strip() if soup.title and soup.title.string else None
                    industry = None
                    for meta in soup.find_all("meta"):
                        if meta.get("name", "").lower() == "keywords":
                            industry = meta.get("content", "").strip()
                            break
                    company_name = clean_company_name(truncate(company_name, MAX_COMPANY_LEN))
                    target_url = truncate(target_url, MAX_URL_LEN)
                    industry = truncate(industry, MAX_INDUSTRY_LEN)
                    if company_name:
                        batch_records.append({
                            "url": target_url,
                            "company_name": company_name,
                            "industry": industry,
                            "extracted_at": datetime.utcnow()
                        })
                    if len(batch_records) >= BATCH_SIZE:
                        df = pd.DataFrame(batch_records)
                        df = df.drop_duplicates(subset="url", keep="last")
                        for _, row in df.iterrows():
                            cursor.execute("""
                                INSERT INTO raw_common_crawl (url, company_name, industry, extracted_at)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (url) DO NOTHING
                            """, (row['url'], row['company_name'], row['industry'], row['extracted_at']))
                        conn.commit()
                        processed_count += len(df)
                        batch_records = []
                except Exception as inner_e:
                    logging.warning(f"Skipping a record due to error: {inner_e}")
                    continue
            # Final flush
            if batch_records:
                df = pd.DataFrame(batch_records)
                df = df.drop_duplicates(subset="url", keep="last")
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO raw_common_crawl (url, company_name, industry, extracted_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (url) DO NOTHING
                    """, (row['url'], row['company_name'], row['industry'], row['extracted_at']))
                conn.commit()
                processed_count += len(df)
        cursor.close()
        conn.close()
        logging.info(f"Finished processing {processed_count} unique records.")

    except Exception as e:
        logging.error(f"Error during WARC processing: {e}")

def main():
    base_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224644309.7/warc/CC-MAIN-20230528150639-20230528180639-{:05d}.warc.gz"
    
    # Loop through 10 to 500 (inclusive), zero-padded to 5 digits
    for i in range(10, 501):
        warc_url = base_url.format(i)
        logging.info(f"Starting processing for WARC file number: {i}")
        download_and_process_warc(warc_url)

if __name__ == "__main__":
    main()
