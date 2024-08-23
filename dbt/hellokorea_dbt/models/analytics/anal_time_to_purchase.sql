SELECT apl.user_id, asl.searched_ts, apl.purchased_ts, (apl.purchased_ts - asl.searched_ts) as time_to_purchase
from {{ ref('fact_airline_purchase_log') }} apl
join (SELECT asl.user_id, min(asl.searched_ts) as searched_ts from {{ ref('fact_airline_search_log') }} asl GROUP by asl.user_id) asl
on apl.user_id = asl.user_id