{{ config(
    materialized='external',
    format = 'parquet',
    location = 's3://govbr-data/RAW/TSE/CANDIDATOS/file.parquet',
    mode = "overwrite" 
) }}

WITH raw_candidatos AS (
    SELECT * FROM {{ source('landing_tse', 'candidatos') }}
)
SELECT
    *
FROM raw_candidatos