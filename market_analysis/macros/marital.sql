{% macro normalize_marital_status(column_name) %}
    case
        when upper(trim({{ column_name }})) = 'S'
        then 'Single'
        when upper(trim({{ column_name }})) = 'M'
        then 'Married'
        else 'n/a'
    end
{% endmacro %}
