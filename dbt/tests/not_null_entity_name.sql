{{ config(severity='error') }}

SELECT *
FROM {{ ref('unified_companies') }}
WHERE company_name IS NULL