{% test check_consist_len(model, column_name, len) %}

	SELECT
		{{ column_name }}
	FROM
		{{ model }}
	WHERE
		LENGTH({{ column_name }}) != {{ len }}
		
{% endtest %}
