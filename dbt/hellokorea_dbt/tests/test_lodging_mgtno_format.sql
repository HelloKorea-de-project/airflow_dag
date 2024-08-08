WITH split_ids AS (
    SELECT
        mgtno,
        LENGTH(mgtno) - LENGTH(REPLACE(mgtno, '-', '')) + 1 AS part_count
    FROM {{ ref('fresh_lodging') }}
)

SELECT *
FROM split_ids
WHERE part_count != 4