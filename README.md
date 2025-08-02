Australian Company Data Integration ETL
=======================================

Overview
--------

A robust, scalable ETL pipeline to extract, clean, reconcile, and serve unified records of Australian companies from two authoritative data sources: **Common Crawl** (web company presence) and the **Australian Business Register (ABR)** (official registry). The project leverages Python, Airflow, dbt, PostgreSQL, and open-source LLMs for advanced entity resolution.

1\. Database Schema — PostgreSQL DDL
------------------------------------

All DDL files are in [/sql/schema.sql].

**Key Tables**:

*   raw\_common\_crawlStores website, company name, and industry data crawled from .au domains.
    
*   raw\_abrStores bulk-extracted company registry records from Australian Business Register XML.
    
*   unified\_companiesMerged, high-confidence, AI-matched entity records.
    
*   match\_logAudit trail with confidence scores, match rationales, and provenance.
    

<pre> CREATE TABLE raw_common_crawl (
    id SERIAL PRIMARY KEY,
    url VARCHAR(2048) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    industry VARCHAR(1000),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw_abr (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(11) NOT NULL UNIQUE,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(50),
    entity_status VARCHAR(50),
    address VARCHAR(255),
    postcode VARCHAR(10),
    state VARCHAR(10),
    start_date DATE,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE unified_companies (
    unified_id SERIAL PRIMARY KEY,
    abn VARCHAR(11) UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(50),
    entity_status VARCHAR(50),
    address VARCHAR(255),
    postcode VARCHAR(10),
    state VARCHAR(10),
    start_date DATE,
    industry VARCHAR(100),
    website_url VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE match_log (
    id SERIAL PRIMARY KEY,
    common_crawl_id INT REFERENCES raw_common_crawl(id) ON DELETE CASCADE,
    abr_id INT REFERENCES raw_abr(id) ON DELETE CASCADE,
    unified_id INT REFERENCES unified_companies(unified_id) ON DELETE CASCADE,
    match_confidence FLOAT,
    match_reason TEXT,
    matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
 </pre>  `

2\. Pipeline Architecture Diagram & Description
-----------------------------------------------



<pre> [Common Crawl]       [ABR XML Bulk]
      |                    |
+-----v----+         +-----v-----+
| Extract  |         |  Extract  |
+-----+----+         +-----+-----+
      |                    |
    [raw_common_crawl]  [raw_abr]
           \            /
            \          /
          +--v--------v--+
          | Transform/Clean|
          +-------+-------+
                  |
           [Blocking, Fuzzy]
                  |
          [LLM-Aided Matching]
                  |
           [unified_companies]
                  |
          [dbt tests, marts]
                  |
             [FastAPI API]
</pre>
**Description**: Data moves from raw extraction, through normalization, candidate blocking and fast fuzzy match, into an AI/LLM match phase for ambiguous cases, finally unified for analytics and served over an API. 

3\. Technology Justification
----------------------------

| Layer         | Technology                              | Justification                                  |
|---------------|-----------------------------------------|------------------------------------------------|
| Orchestration | Apache Airflow                          | Scheduling, retries, and pipeline management   |
| Extraction    | Python (requests, bs4, warcio, lxml)    | Fast, scriptable, well-supported parsing       |
| Matching      | RapidFuzz, Hugging Face (Mistral-7B-Instruct) | Open-source, cost-free, & accurate     |
| Data Quality  | dbt                                     | Industry standard for transformation/testing    |
| Storage       | PostgreSQL                              | Robust, ACID-compliant, scalable               |
| Serving       | FastAPI                                 | Modern, async API for analytics/BI             |


4\. AI Model Used & Rationale
-----------------------------

*   **LLM Used:** Hugging Face API with [Mistral-7B-Instruct](https://huggingface.co/mistralai/Mistral-7B-Instruct-v0.2)
    
*   **Why:**
    
    *   Cost-free for research/dev use (open weights, community inference endpoint)
        
    *   Excellent at general-purpose English and semi-structured entity reasoning
        
    *   Flexible prompt design, can be swapped for other open models as needed
        
*   **How LLM Is Used:**
    
    *   Only the top candidate fuzzy matches between sources are uploaded to the LLM for high-confidence matching and rationale scoring
        
    *   The LLM decides match, confidence, and writes explainable justification stored in match\_log
        

5\. Setup & Running Instructions
--------------------------------

**Dev Environment:**

*   Recommended: VS Code or Jupiter Lab
    
*   All scripts tested under Unix shell, Python venv
    

**1\. Clone Repo**

<pre>
    git clone https://github.com/yourgithub/australian_company_etl.git
    cd australian_company_etl
</pre>

**2\. Create Python Environment & Install Dependencies**

<pre> python3 -m venv venv  source venv/bin/activate  
      pip install -r requirements.txt   `</pre>

**3\. Start/PostgreSQL & Create Tables**

<pre>   psql -U postgres -d company_db -f sql/schema.sql  
       psql -U postgres -d company_db -f sql/roles.sql   `</pre>

**4\. Configure env variables** (.env or system ENV)

*   HUGGINGFACE\_API\_KEY — for entity matching
    
*   Database credentials as needed
    

**5\. Run the ETL Pipeline**

*   Use Airflow: place DAG in airflow/, run scheduler
    
*   Or run individually:
    
    *   python scripts/extract\_common\_crawl.py
        
    *   python scripts/extract\_abr.py
        
    *   python scripts/entity\_matching.py --api\_key $HUGGINGFACE\_API\_KEY
        

**6\. Run dbt Models and Tests**

<pre>cd dbt  
     dbt run  
     dbt test   </pre>`

**7\. Query API**
<pre>   
cd ..
uvicorn main:app  # Access localhost:8000/docs for Swagger UI   `</pre>

6\. Python Scripts for Data Extraction
--------------------------------------

All found in [/scripts/]

*   extract\_common\_crawl.py: Extracts and cleans .au company websites from WARC files in batch.
    
*   extract\_abr.py: Parses ABR XML bulk files to staging table.
    
*   entity\_matching.py: Fuzzy and LLM-powered entity resolver (Hugging Face model).
    
*   etl\_pipeline\_dag.py: Airflow DAG script orchestrating the complete flow.
    

7\. PostgreSQL DDL
------------------

See [/sql/schema.sql] for core table definitions. Indexes and constraints included for fast search/match.

8\. ETL Pipeline Code
---------------------

*   Main pipeline orchestrated via Airflow (airflow/etl\_pipeline\_dag.py)
    
*   Each script modular, can be run standalone for dev/test
    
*   Bulk processing, parallelization, retry logic incorporated for high reliability
    

9\. dbt Models & Tests
----------------------

*   Found in [/dbt/models/staging/] (staging) and [/dbt/models/marts/] (marts)
    
*   Prebuilt tests: not\_null, unique, referential integrity
    
*   Transformations: Field normalization (lowercase, trimming), entity deduplication, valid ABN/postcode checks
    
 `

10\. Design Choices & Entity Matching Approach
----------------------------------------------

**Design Rationale**:

*   Staging-then-transform approach for data quality, audit, and ease of reruns
    
*   Fuzzy + LLM hybrid entity matching yields high accuracy, full explainability, and minimizes API/resource cost
    
*   dbt for analytical quality and transformation transparency
    
*   Airflow for reliability and recoverability on large daily or batch runs
    

**Entity Matching**:

*   Normalize fields (lowercase, strip suffixes)
    
*   Block on easy-to-map features (prefix, postcode)
    
*   Fuzzy match for top N candidates
    
*   LLM (Mistral-7B) for ambiguous/close matches only
    
*   Store full confidence and rationale per match
    

11\. IDE Used for Development
-----------------------------

*   **Visual Studio Code** (recommended: Python + dbt plugins enabled)
    
*   Also tested with JupyterLab for prototyping
    

Folder Structure (Best Practice)
--------------------------------
<pre>  
/
├── airflow/
├── api/
├── dbt/
├── diagrams/ or output/    # Place your screenshots here!
├── scripts/
├── sql/
├── README.md
├── requirements.txt
└── .env
</pre>   `

License
-------

This project is released under the MIT License (see LICENSE).

---

## Author

**Sandeep Nidumukkala**    
[sandeepnidumukkla@gmail.com](mailto:sandeepnidumukkla@gmail.com)

