-- assign `column` as a non-enforced primary key in the table
{% macro bq_primary_key(column) %}
    if not exists (
        select *
        from `{{ this.schema }}`.`INFORMATION_SCHEMA`.`TABLE_CONSTRAINTS`
        where
            normalize(constraint_type) = normalize('PRIMARY KEY')
            and table_name = normalize('{{ this.identifier }}')
    )
    then alter table {{ this }} add primary key({{ column }}) not enforced
    ;
    end if
    ;
{% endmacro %}
