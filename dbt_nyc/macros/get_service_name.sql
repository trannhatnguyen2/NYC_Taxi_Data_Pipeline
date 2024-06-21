{#
    this macro returns service name
#}

{% macro get_service_name(service_type) %}
    case {{ service_type }}
        when 1 then 'Yellow'
        when 2 then 'Green'
    end
{% endmacro %}