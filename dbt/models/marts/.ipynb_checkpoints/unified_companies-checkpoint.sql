{{ config(materialized='table') }}

SELECT
    unified_id,
    abn,
    company_name,
    entity_type,
    entity_status,
    address,
    postcode,
    state,
    start_date,
    industry,
    website_url,
    created_at,
    updated_at
FROM {{ source('public', 'unified_companies') }}
WHERE company_name IS NOT NULL