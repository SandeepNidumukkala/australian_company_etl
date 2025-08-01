{{ config(materialized='view') }}

SELECT
    id,
    abn,
    TRIM(LOWER(entity_name)) AS entity_name,
    entity_type,
    entity_status,
    TRIM(LOWER(address)) AS address,
    postcode,
    state,
    start_date,
    extracted_at
FROM {{ source('public', 'raw_abr') }}
WHERE abn ~ '^[0-9]{11}$'
  AND entity_name IS NOT NULL