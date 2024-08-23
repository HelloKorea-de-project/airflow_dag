select depart_airport_code, count(*) cnt_dep
from {{ ref('fact_airline_click_log') }} acl
where clicked_ts >= CURRENT_TIMESTAMP - INTERVAL ' 15 days'
group by depart_airport_code
order by cnt_dep desc;