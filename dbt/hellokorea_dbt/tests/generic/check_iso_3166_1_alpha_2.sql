{% test check_iso_3166_1_alpha_2(model, column_name) %}

	SELECT
		{{ column_name }}
	FROM
		{{ model }}
	WHERE
		LENGTH({{ column_name }}) != 2 OR
		SUBSTRING({{ column_name }}, 1, 3) = LOWER(SUBSTRING({{ column_name }}, 1, 3))

{% endtest %}

