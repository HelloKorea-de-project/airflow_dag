{% test check_contain(model, column_name, sub_str, start_pos) %}

    SELECT 
        {{ column_name }}
    FROM
        {{ model }}
    WHERE 
        SUBSTRING({{ column_name }}, {{ start_pos }}, LENGTH({{ sub_str }})) != {{ sub_str }}

{% endtest %}
