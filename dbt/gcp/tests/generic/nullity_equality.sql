{%- test nullity_equality(model, compare_model, keys, columns=none, exclude=none) -%}

    {#- A dbt-utils trick -#}
    {#- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. -#}
    {%- if not execute %} {{ return('') }} {% endif -%}

    {%- set columns_a = adapter.get_columns_in_relation(model) | map(attribute="column") | list -%}
    {%- set columns_b = adapter.get_columns_in_relation(compare_model) | map(attribute="column") | list -%}

    {#- check keys -#}
    {%- if not keys -%}
        {{ exceptions.raise_compiler_error("Keys can not be none.") }}
    {%- endif -%}
    {%- if not is_subset(keys, columns_a) -%}
        {{ print("* keys: " ~ keys) }}
        {{ print("* model columns: " ~ columns_a) }}
        {{ exceptions.raise_compiler_error("Each key should be a column of model " ~ model) }}
    {%- endif -%}
    {%- if not is_subset(keys, columns_b) -%}
        {{ print("* keys: " ~ keys) }}
        {{ print("* model columns: " ~ columns_b) }}
        {{ exceptions.raise_compiler_error("Each key" ~ keys ~ " should be a column of model " ~ compare_model) }}
    {%- endif -%}

    {%- if exclude is none -%} {%- set exclude = [] -%} {%- endif -%}
    {%- if columns is none -%}
        {%- set columns = columns_a | reject("in", exclude) | list -%}
    {%- endif -%}

    {#- check columns -#}
    {%- if not is_subset(columns, columns_a) -%}
        {{ exceptions.raise_compiler_error("Each comparing column " ~ keys ~ " should be a column of model " ~ model) }}
    {%- endif -%}
    {%- if not is_subset(columns, columns_b) -%}
        {{ exceptions.raise_compiler_error("Each comparing column " ~ keys ~ " should be a column of model " ~ compare_model) }}
    {%- endif -%}

    with
        {% for c in columns %}
            nullity_{{ c }} as (
                select
                    {% for k in keys %} a.{{ k }} as {{ k }}, {% endfor %}
                    '{{ c }}' as inconsistent_column,
                    (select * from unnest([format('%T', a.{{ c }})])) as value_a,
                    (select * from unnest([format('%T', b.{{ c }})])) as value_b
                from {{ model }} as a
                inner join
                    {{ compare_model }} as b
                    on {% for k in keys %}
                        a.{{ k }} = b.{{ k }} {%- if not loop.last %} and {% endif %}
                    {% endfor %}
                where (a.{{ c }} is null) != (b.{{ c }} is null)
            )
            {%- if not loop.last %}, {% endif %}
        {% endfor %}

    {% for c in columns %}
        select *
        from nullity_{{ c }}
        {%- if not loop.last %}
            union all
        {% endif %}
    {% endfor %}

{%- endtest -%}


-- A sample output from "dbt show -s"
--
-- Previewing node 'nullity_equality_stg_listings_source_dataset_raw_listings___id':
-- |                  id | inconsistent_column | value_a | value_b |
-- | ------------------- | ------------------- | ------- | ------- |
-- |  959587386995646967 | host_verifications  | []      | NULL    |
-- |  925722812381259994 | host_verifications  | []      | NULL    |
-- |  898285895183186184 | host_verifications  | []      | NULL    |
-- | 1030341499086695061 | host_verifications  | []      | NULL    |
-- |  884305142367954652 | host_verifications  | []      | NULL    |

