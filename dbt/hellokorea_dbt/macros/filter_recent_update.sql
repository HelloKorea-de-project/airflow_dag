{% macro filter_recent_update(column_name, days_ago) %}

  {{ column_name }} >= CURRENT_DATE - INTERVAL '{{ days_ago }} days'

{% endmacro %}
