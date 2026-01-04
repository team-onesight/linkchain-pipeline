WITH source AS (SELECT
                    log_id
                  , link_id
                  , error_message
                  , error_type
                  , created_at
                FROM {{ source('raw_data', 'crawl_failure_log') }}
)
SELECT *
FROM source
