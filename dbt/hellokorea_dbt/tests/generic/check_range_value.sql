{% test check_range_value(model, column_name, min_num, max_num) %}
	
	SELECT
		{{ column_name }}
	FROM
		{{ model }}
	WHERE
		({{ min_num }} > {{ column_name }}) OR ({{ max_num }} < {{ column_name }})

{% endtest %}
