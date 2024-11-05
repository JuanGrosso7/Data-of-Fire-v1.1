{% macro clean_null(value) %}
    CASE 
        WHEN {{ value }} IN ('None', 'NNN None', 'N None') THEN NULL 
        ELSE {{ value }}
    END
{% endmacro %}

