{{ config(severity='error') }}

SELECT abn, COUNT(*) as count
FROM {{ ref('unified_companies') }}
GROUP BY abn
HAVING COUNT(*) > 1