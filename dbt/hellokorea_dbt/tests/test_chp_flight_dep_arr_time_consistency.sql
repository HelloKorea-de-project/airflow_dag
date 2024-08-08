SELECT 
    id
FROM {{ ref('fresh_chp_flight') }}
WHERE depTime >= arrTime
