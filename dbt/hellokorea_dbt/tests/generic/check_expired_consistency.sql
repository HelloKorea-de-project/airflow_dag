{% test check_expired_consistency(model, column_dict) %}
{% set date_col_name = column_dict['date_col_name'] %}
{% set is_expired = column_dict['is_expired'] %}

        WITH consistency_check AS (
		              SELECT 
			                  {{ date_col_name }}, 
					                  COUNT(DISTINCT {{ is_expired }}) AS unique_isexpired_count
							              FROM {{ model }}
								              GROUP BY {{ date_col_name }}
									        )
										        SELECT COUNT(*)
											        FROM consistency_check
												        WHERE unique_isexpired_count > 1
													{% endtest %}
