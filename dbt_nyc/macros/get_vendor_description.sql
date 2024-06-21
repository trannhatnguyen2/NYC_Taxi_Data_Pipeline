{#
    this macro returns vendor description
#}

{% macro get_vendor_description(vendor_id) %}
    case {{ vendor_id }}
        when 1 then 'Creative Mobile Technologies, LLC'
        when 2 then 'VeriFone Inc'
    end
{% endmacro %}