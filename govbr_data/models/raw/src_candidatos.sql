{{ config(
    materialized='external',
    plugin = 'delta',
    location = 's3://govbr-data/RAW/TSE/CANDIDATOS/',
    mode = "overwrite" 
) }}

WITH raw_candidatos AS (
    SELECT * FROM {{ source('landing_tse', 'candidatos') }}
)
SELECT
    *
FROM raw_candidatos