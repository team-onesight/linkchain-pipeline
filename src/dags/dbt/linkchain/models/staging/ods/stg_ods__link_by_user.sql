WITH source AS (SELECT
                    link_id
                  , url
                  , title
                  , description
                  , views
                  , created_by_user_id
                  , created_by_username
                  , created_at
                  , link_embedding
                  , image_url
                FROM {{ source('ods', 'link') }}
)

SELECT *
FROM source as t1
where t1.created_by_user_id IS NOT NULL
