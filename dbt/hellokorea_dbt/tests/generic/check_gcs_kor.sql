{% test check_gcs_kor(model, lat_col_name, lon_col_name) %}

	SELECT
		{{ lat_col_name }},
		{{ lon_col_name }}
	FROM
		{{ model }}
	WHERE
		{{ lat_col_name }} < 30 OR {{ lat_col_name }} > 40 OR
		{{ lon_col_name }} < 120 OR {{ lon_col_name }} > 135

{% endtest %}
