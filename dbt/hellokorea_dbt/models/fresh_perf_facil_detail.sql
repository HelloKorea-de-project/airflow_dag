SELECT * 
FROM {{ source('raw_data', 'perf_facil_detail') }}
WHERE {{ filter_recent_update('updatedat', 1) }}