{{ config(
    materialized='incremental',
    unique_key='created_date',
) }}

with base_links as (
    select
        cast(created_at as date) as created_date
        , link_id
    from {{ ref('combined_links') }}
    
    {% if var('check_date', none) %}
    where cast(created_at as date) = '{{ var("check_date") }}'
    {% endif %}
)
select
    created_date
    , count(*) as total_links_added
    , count(distinct link_id) as unique_links_added
from base_links
group by 1
