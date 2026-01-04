{{ config(
    materialized='view',
    schema='RAW_DATA'
) }}

WITH combined AS (
    SELECT * FROM {{ ref('combined_links') }}
),

metadata AS (
SELECT link_id FROM {{ ref('stg_raw_data__crawled_html_metadata') }}
    ),

    failures AS (
SELECT
    link_id
FROM {{ ref('stg_raw_data__crawl_failure_log') }}
GROUP BY link_id
HAVING COUNT(*) >= 1
    )
SELECT
    t.link_id,
    t.url,
    t.created_at
FROM combined t
WHERE NOT EXISTS (
    SELECT 1 FROM metadata m WHERE m.link_id = t.link_id
)
  AND NOT EXISTS (
    SELECT 1 FROM failures f WHERE f.link_id = t.link_id
)
