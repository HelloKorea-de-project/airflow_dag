WITH split_ids AS (
	SELECT
		id,
		LENGTH(id) - LENGTH(REPLACE(id, '-', '')) + 1 AS part_count
		FROM {{ ref('chp_flight') }}
)

SELECT *
FROM split_ids
WHERE part_count != 7
