SELECT * 
FROM {{ source('raw_data', 'arr_cnt_icn') }}
WHERE {{ filter_recent_update('extracteddate', 3) }}
