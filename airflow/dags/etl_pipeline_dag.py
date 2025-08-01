from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys

# Add your repo to the path, so Airflow can import your modules
sys.path.append('/Users/sandeepnidumukkala/australian_company_etl/scripts')

from extract_common_crawl import download_and_process_warc
from extract_abr import extract_abr
from entity_matching import entity_matching

# Use HUGGINGFACE_API_KEY from the Airflow connection or set as an environment variable (preferred)
import os
HUGGINGFACE_API_KEY = os.getenv('HUGGINGFACE_API_KEY', 'your-hf-key-here')  # Replace as needed

WARc_URL = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/segments/1685224644309.7/warc/CC-MAIN-20230528150639-20230528180639-00062.warc.gz"

with DAG(
    'etl_pipeline_dag',
    start_date=datetime(2025, 7, 30),
    schedule_interval='@daily',
    default_args={
        'retries': 2,
        'retry_delay': 60,  # seconds between retries
    },
    catchup=False
) as dag:
    
    # 1. Extract from Common Crawl
    extract_common_crawl_task = PythonOperator(
        task_id='extract_common_crawl',
        python_callable=download_and_process_warc,
        op_args=[WARc_URL]
    )
    
    # 2. Extract from ABR
    extract_abr_task = PythonOperator(
        task_id='extract_abr',
        python_callable=extract_abr
    )
    
    # 3. Entity Matching with Hugging Face LLM (only, no xAI/OpenAI)
    entity_match_task = PythonOperator(
        task_id='entity_matching',
        python_callable=entity_matching,
        op_kwargs={'api_key': HUGGINGFACE_API_KEY}
    )
    
    # 4. dbt run (adjust path as needed)
    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /Users/sandeepnidumukkala/australian_company_etl/dbt && dbt run'
    )
    
    # 5. dbt test
    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /Users/sandeepnidumukkala/australian_company_etl/dbt && dbt test'
    )
    
    # Set task dependencies
    extract_common_crawl_task >> extract_abr_task >> entity_match_task >> dbt_run_task >> dbt_test_task
