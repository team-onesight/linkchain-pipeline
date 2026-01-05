SELECT *
from {{ ref('fct_daily_link_counts') }}
where total_links_added <= 0
  and created_date = '{{ var("check_date") }}'
