{%- macro get_table_types_sql() -%}
  {{ return(adapter.dispatch('get_table_types_sql')()) }}
{%- endmacro -%}

{% macro default__get_table_types_sql() %}
            case table_type
                when 'BASE TABLE' then 'table'
                when 'EXTERNAL TABLE' then 'external'
                when 'MATERIALIZED VIEW' then 'materializedview'
                else lower(table_type)
            end as {{ adapter.quote('table_type') }}
{% endmacro %}


{% macro postgres__get_table_types_sql() %}
            case table_type
                when 'BASE TABLE' then 'table'
                when 'FOREIGN' then 'external'
                when 'MATERIALIZED VIEW' then 'materializedview'
                else lower(table_type)
            end as {{ adapter.quote('table_type') }}
{% endmacro %}


{% macro get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}
    {{ return(adapter.dispatch('get_tables_by_pattern_sql')
        (schema_pattern, table_pattern, exclude, database)) }}
{% endmacro %}

{% macro default__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}

        select distinct
            table_schema as {{ adapter.quote('table_schema') }},
            table_name as {{ adapter.quote('table_name') }},
            {{ get_table_types_sql() }}
        from {{ database }}.information_schema.tables
        where table_schema ilike '{{ schema_pattern }}'
        and table_name ilike '{{ table_pattern }}'
        and table_name not ilike '{{ exclude }}'

{% endmacro %}

{% macro get_relations_by_pattern(schema_pattern, table_pattern, exclude='', database=target.database) %}
    {{ return(adapter.dispatch('get_relations_by_pattern')(schema_pattern, table_pattern, exclude, database)) }}
{% endmacro %}

{% macro default__get_relations_by_pattern(schema_pattern, table_pattern, exclude='', database=target.database) %}

    {%- call statement('get_tables', fetch_result=True) %}

      {{ get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude, database) }}

    {%- endcall -%}

    {%- set table_list = load_result('get_tables') -%}

    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table'] -%}
            {%- set tbl_relation = api.Relation.create(
                database=database,
                schema=row.table_schema,
                identifier=row.table_name,
                type=row.table_type
            ) -%}
            {%- do tbl_relations.append(tbl_relation) -%}
        {%- endfor -%}

        {{ return(tbl_relations) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}

{% macro data_type_format_source(column) -%}
  {{ return(adapter.dispatch('data_type_format_source')(column)) }}
{%- endmacro %}

{% macro format_column(column) -%}
  {% set data_type = column.dtype %}
  {% set formatted = column.column.lower() ~ " " ~ data_type %}
  {{ return({'name': column.name, 'data_type': data_type, 'formatted': formatted}) }}
{%- endmacro -%}
{% macro default__data_type_format_source(column) %}
    {% set formatted = format_column(column) %}
    {{ return(formatted['data_type'] | lower) }}
{% endmacro %}


{% macro get_tables_in_schema(schema_name, database_name=target.database, table_pattern='%', exclude='') %}
    
    {% set tables=get_relations_by_pattern(
        schema_pattern=schema_name,
        database=database_name,
        table_pattern=table_pattern,
        exclude=exclude
    ) %}

    {% set table_list= tables | map(attribute='identifier') %}

    {{ return(table_list | sort) }}

{% endmacro %}



{% macro generate_source_local(schema_name, database_name=target.database, generate_columns=False, include_descriptions=False, include_data_types=True, table_pattern='%', exclude='', name=schema_name, table_names=None, include_database=False, include_schema=False) %}
    {{ return(adapter.dispatch('generate_source_local')(schema_name, database_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema)) }}
{% endmacro %}

{% macro default__generate_source_local(schema_name, database_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema) %}


{% set sources_yaml=[] %}
{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ name | lower) %}

{% if include_descriptions %}
    {% do sources_yaml.append('    description: ""' ) %}
{% endif %}

{% if database_name != target.database or include_database %}
{% do sources_yaml.append('    database: ' ~ database_name | lower) %}
{% endif %}

{% if schema_name != name or include_schema %}
{% do sources_yaml.append('    schema: ' ~ schema_name | lower) %}
{% endif %}

{% do sources_yaml.append('    tables:') %}

{% if table_names is none %}
{% set tables=get_tables_in_schema(schema_name, database_name, table_pattern, exclude) %}
{% else %}
{% set tables = table_names %}
{% endif %}


{% for table in tables %}
    {% do sources_yaml.append('      - name: ' ~ table | lower ) %}
    {% if include_descriptions %}
        {% set results = run_query("SELECT description 
                                    FROM pg_catalog.pg_description 
                                    WHERE objoid = (
                                        SELECT oid 
                                        FROM pg_class 
                                        WHERE relname = '" ~ table ~ "' AND relnamespace = (
                                            SELECT oid 
                                            FROM pg_catalog.pg_namespace 
                                            WHERE nspname = '" ~ schema_name ~ "'
                                        )
                                    ) AND objsubid = 0;") %}
        {% set table_desc = results.columns[0].values()[0] %}
        {% if not table_desc %}
            {% do sources_yaml.append('        description: ""' ) %}
        {% else %}
            {% do sources_yaml.append('        description: "' ~ table_desc ~ '"' ) %}
        {% endif %}
    {% endif %}
    {% if generate_columns %}
    {% do sources_yaml.append('        columns:') %}

        {% set table_relation=api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=table
        ) %}

        {% set columns=adapter.get_columns_in_relation(table_relation) %}

        {% for column in columns %}
            {% do sources_yaml.append('          - name: ' ~ column.name | lower ) %}
            {% if include_data_types %}
                {% do sources_yaml.append('            data_type: ' ~ data_type_format_source(column)) %}
            {% endif %}
            {% if include_descriptions %}
                {% set results = run_query("SELECT description FROM pg_catalog.pg_description WHERE objsubid = 
                                            (
                                                SELECT ordinal_position FROM information_schema.columns WHERE table_name='" ~ table ~ "' AND column_name='" ~ column.name ~ "'
                                            ) 
                                            and objoid = 
                                            (
                                                SELECT oid FROM pg_class WHERE relname ='" ~ table ~ "' AND relnamespace = 
                                                (
                                                    SELECT oid FROM pg_catalog.pg_namespace WHERE nspname ='" ~ schema_name ~ "'
                                                )
                                        );") %}
                {% if not results %}
                    {% do sources_yaml.append('            description: ""' ) %}
                {% else %}
                    {% set col_desc = results.columns[0].values()[0] %}
                    {% do sources_yaml.append('            description: "' ~ col_desc ~ '"') %}
                {% endif %}
            {% endif %}
        {% endfor %}
            {% do sources_yaml.append('') %}

    {% endif %}

{% endfor %}

{% if execute %}

    {% set joined = sources_yaml | join ('\n') %}
    {{ print(joined) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}