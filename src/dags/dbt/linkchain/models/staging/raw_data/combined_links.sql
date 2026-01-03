{{ config(
    materialized='view',
    schema='RAW_DATA'
) }}

WITH ods_links AS (
    SELECT
        link_id
      , url
      , title
      , description
      , link_embedding
      , created_at
    FROM {{ ref('stg_ods__link') }}
),

raw_urls AS (
SELECT
    link_id
  , url
  , NULL AS title
  , NULL AS description
  , NULL AS link_embedding
  , created_at
FROM {{ ref('stg_raw_data__url_crawled') }}
    )

SELECT * FROM ods_links
UNION ALL
SELECT * FROM raw_urls
