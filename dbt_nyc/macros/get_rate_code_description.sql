{#
    this macro returns rate_code description
#}

{% macro get_rate_code_description(rate_code_id) %}
    case {{ rate_code_id }}
        when 1 then 'Standard rate'
        when 2 then 'JFK'
        when 3 then 'Newark'
        when 4 then 'Nassau or Westchester'
        when 5 then 'Negoriated fare'
        when 6 then 'Group ride'
    end
{% endmacro %}