-- auto-generated definition
WITH source AS (SELECT
                    link_id
                  , s3_path
                  , file_size
                  , created_at
                  , updated_at
                FROM {{ source('raw_data', 'crawled_html_metadata') }}
)
SELECT *
FROM source

