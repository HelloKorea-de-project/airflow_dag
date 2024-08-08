{% test check_uniq_comb_key(model, combination) %}

	with validation as (
		select
			{{ combination | join(', ') }},
			count(*) as occur_cnt
		from {{ model }}
		group by {{ combination | join(', ') }}
	)

	select *
	from validation
	where occur_cnt > 1

{% endtest %}
