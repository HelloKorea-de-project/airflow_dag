{% test check_date(model, column_name) %}

	SELECT
		{{ column_name }}
	FROM
		{{ model }}
	WHERE
		{{ column_name }} < '2000-01-01'::date
		
{% endtest %}
