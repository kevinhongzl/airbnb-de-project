-- assign a foreign key in the table 
{% macro bq_foreign_key(fk_column_name, pk_table_name, pk_column_name=none) %}
    {% if pk_column_name is none %}
        {% set pk_column_name = fk_column_name %}
    {% endif %}

    if not exists (
        select
            tc.constraint_name as contraint_name,
            kcu.table_name as constrained_table_name,
            kcu.column_name as fk_column_name,
            ccu.table_name as pk_table_name,
            ccu.column_name as pk_column_name
        from `{{ this.schema }}`.`INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` as kcu
        inner join
            `{{ this.schema }}`.`INFORMATION_SCHEMA`.`CONSTRAINT_COLUMN_USAGE` as ccu
            on kcu.constraint_name = ccu.constraint_name
        inner join
            `{{ this.schema }}`.`INFORMATION_SCHEMA`.`TABLE_CONSTRAINTS` as tc
            on ccu.constraint_name = tc.constraint_name
        where
            normalize(tc.constraint_type) = normalize('FOREIGN KEY')
            and normalize(kcu.table_name) = normalize('{{ this.identifier }}')
            and normalize(kcu.column_name) = normalize('{{ fk_column_name }}')
            and normalize(ccu.table_name) = normalize('{{ pk_table_name }}')
    )
    then
        alter table {{ this }} add foreign key(
            {{ fk_column_name }}
        ) references {{ ref(pk_table_name) }} ({{ pk_column_name }})
        not enforced
    ;
    end if
    ;
{% endmacro %}
