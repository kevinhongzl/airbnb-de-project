-- return the boolean value of whether list1 is a subset of list2
{% macro is_subset(list1, list2) %}
    {% for x in list1 %}
        {% if x not in list2 %} {{ return(False) }} {% endif %}
    {% endfor %}
    {{ return(True) }}
{% endmacro %}
