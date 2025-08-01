{{ config(materialized='view') }}

SELECT
    id,
    TRIM(LOWER(url)) AS url,
    TRIM(LOWER(company_name)) AS company_name,
    TRIM(LOWER(industry)) AS industry,
    extracted_at
FROM {{ source('public', 'raw_common_crawl') }}
WHERE company_name IS NOT NULL
  AND url ~* '\.au$'