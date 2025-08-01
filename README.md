Australian Company Data Integration ETL
=======================================

Overview
--------

A robust, scalable ETL pipeline to extract, clean, reconcile, and serve unified records of Australian companies from two authoritative data sources: **Common Crawl** (web company presence) and the **Australian Business Register (ABR)** (official registry). The project leverages Python, Airflow, dbt, PostgreSQL, and open-source LLMs for advanced entity resolution.

1\. Database Schema — PostgreSQL DDL
------------------------------------

All DDL files are in [/sql/schema.sql](https://www.perplexity.ai/search/sql/schema.sql).

**Key Tables**:

*   raw\_common\_crawlStores website, company name, and industry data crawled from .au domains.
    
*   raw\_abrStores bulk-extracted company registry records from Australian Business Register XML.
    
*   unified\_companiesMerged, high-confidence, AI-matched entity records.
    
*   match\_logAudit trail with confidence scores, match rationales, and provenance.
    

Click to view core DDL

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   sqlCREATE TABLE raw_common_crawl (      id SERIAL PRIMARY KEY,      url VARCHAR(2048) NOT NULL UNIQUE,      company_name VARCHAR(255),      industry VARCHAR(1000),      extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  );  CREATE TABLE raw_abr (      id SERIAL PRIMARY KEY,      abn VARCHAR(11) NOT NULL UNIQUE,      entity_name VARCHAR(255) NOT NULL,      entity_type VARCHAR(50),      entity_status VARCHAR(50),      address VARCHAR(255),      postcode VARCHAR(10),      state VARCHAR(10),      start_date DATE,      extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  );  CREATE TABLE unified_companies (      unified_id SERIAL PRIMARY KEY,      abn VARCHAR(11) UNIQUE,      company_name VARCHAR(255) NOT NULL,      entity_type VARCHAR(50),      entity_status VARCHAR(50),      address VARCHAR(255),      postcode VARCHAR(10),      state VARCHAR(10),      start_date DATE,      industry VARCHAR(100),      website_url VARCHAR(512),      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  );  CREATE TABLE match_log (      id SERIAL PRIMARY KEY,      common_crawl_id INT REFERENCES raw_common_crawl(id) ON DELETE CASCADE,      abr_id INT REFERENCES raw_abr(id) ON DELETE CASCADE,      unified_id INT REFERENCES unified_companies(unified_id) ON DELETE CASCADE,      match_confidence FLOAT,      match_reason TEXT,      matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  );   `

2\. Pipeline Architecture Diagram & Description
-----------------------------------------------

**Diagram** (text/ASCII version for GitHub; see diagrams/ for PNG/drawio):

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   text[Common Crawl]       [ABR XML Bulk]        |                    |  +-----v----+         +-----v-----+  | Extract  |         |  Extract  |  +-----+----+         +-----+-----+        |                    |      [raw_common_crawl]  [raw_abr]             \            /              \          /            +--v--------v--+            | Transform/Clean|            +-------+-------+                    |             [Blocking, Fuzzy]                    |            [LLM-Aided Matching]                    |             [unified_companies]                    |            [dbt tests, marts]                    |               [FastAPI API]   `

**Description**: Data moves from raw extraction, through normalization, candidate blocking and fast fuzzy match, into an AI/LLM match phase for ambiguous cases, finally unified for analytics and served over an API. See detailed discussion in [docs/ARCHITECTURE.md](https://www.perplexity.ai/search/docs/ARCHITECTURE.md).

3\. Technology Justification
----------------------------

LayerTechnologyJustificationOrchestrationApache AirflowScheduling, retries, and pipeline managementExtractionPython (requests, bs4, warcio, lxml)Fast, scriptable, well-supported parsingMatchingRapidFuzz, Hugging Face (Mistral-7B-Instruct)Open-source, cost-free, & accurateData QualitydbtIndustry standard for transformation/testingStoragePostgreSQLRobust, ACID-compliant, scalableServingFastAPIModern, async API for analytics/BI

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

*   Recommended: VS Code or PyCharm with Python 3.9+
    
*   All scripts tested under Unix shell, Python venv
    

**1\. Clone Repo**

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   bashgit clone https://github.com/yourgithub/australian_company_etl.git  cd australian_company_etl   `

**2\. Create Python Environment & Install Dependencies**

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   bashpython3 -m venv venv  source venv/bin/activate  pip install -r requirements.txt   `

**3\. Start/PostgreSQL & Create Tables**

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   bashpsql -U postgres -d company_db -f sql/schema.sql  psql -U postgres -d company_db -f sql/roles.sql   `

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

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   bashcd dbt  dbt run  dbt test   `

**7\. Query API**

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   bashcd api  uvicorn main:app  # Access localhost:8000/docs for Swagger UI   `

6\. Python Scripts for Data Extraction
--------------------------------------

All found in [/scripts/](https://www.perplexity.ai/search/scripts/):

*   extract\_common\_crawl.py: Extracts and cleans .au company websites from WARC files in batch.
    
*   extract\_abr.py: Parses ABR XML bulk files to staging table.
    
*   entity\_matching.py: Fuzzy and LLM-powered entity resolver (Hugging Face model).
    
*   etl\_pipeline\_dag.py: Airflow DAG script orchestrating the complete flow.
    

7\. PostgreSQL DDL
------------------

See [/sql/schema.sql](https://www.perplexity.ai/search/sql/schema.sql) for core table definitions. Indexes and constraints included for fast search/match.

8\. ETL Pipeline Code
---------------------

*   Main pipeline orchestrated via Airflow (airflow/etl\_pipeline\_dag.py)
    
*   Each script modular, can be run standalone for dev/test
    
*   Bulk processing, parallelization, retry logic incorporated for high reliability
    

9\. dbt Models & Tests
----------------------

*   Found in [/dbt/models/staging/](https://www.perplexity.ai/search/dbt/models/staging/) (staging) and [/dbt/models/marts/](https://www.perplexity.ai/search/dbt/models/marts/) (marts)
    
*   Prebuilt tests: not\_null, unique, referential integrity
    
*   Transformations: Field normalization (lowercase, trimming), entity deduplication, valid ABN/postcode checks
    
*   Example test (dbt/models/schema.yml):
    

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   textmodels:    - name: stg_abr      columns:        - name: abn          tests:            - not_null            - unique   `

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
    
*   Also tested with PyCharm and JupyterLab for prototyping
    

Folder Structure (Best Practice)
--------------------------------

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   textaustralian_company_etl/  ├── airflow/  ├── dbt/  ├── diagrams/  ├── logs/  ├── scripts/  ├── sql/  ├── requirements.txt  ├── README.md  └── .env (excluded from git, stores API keys)   `

License
-------

This project is released under the MIT License (see LICENSE).