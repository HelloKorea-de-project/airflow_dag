SELECT 
    id
FROM {{ ref('chp_flight') }}
WHERE depTime >= arrTime
