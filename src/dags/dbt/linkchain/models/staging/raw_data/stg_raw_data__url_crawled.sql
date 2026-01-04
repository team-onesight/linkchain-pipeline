WITH source AS (SELECT
                    link_id
                  , url
                  , created_at
                  , updated_at


                FROM {{ source('raw_data', 'url_crawled') }}
)
SELECT *
FROM source
