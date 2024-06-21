{#
    this macro returns payment description
#}

{% macro get_payment_description(payment_type_id) %}
    case {{ payment_type_id }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end
{% endmacro %}