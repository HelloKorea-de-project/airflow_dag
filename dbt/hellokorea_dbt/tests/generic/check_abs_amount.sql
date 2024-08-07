{% test check_abs_amount(model, column_name) %}

	SELECT
		{{ column_name }}
	FROM
		{{ model }}
	WHERE
		{{ column_name }} < 0
		
{% endtest %}
