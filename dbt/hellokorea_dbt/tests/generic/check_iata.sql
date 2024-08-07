{% test check_iata(model, column_name) %}

	SELECT
		{{ column_name }}
	FROM
		{{ model }}
	WHERE
		LENGTH({{ column_name }}) != 3 OR
		SUBSTRING({{ column_name }}, 1, 3) = LOWER(SUBSTRING({{ column_name }}, 1, 3))

{% endtest %}
