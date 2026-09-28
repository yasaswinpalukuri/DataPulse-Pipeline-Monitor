{#
  dbt's default names a custom schema "<target_schema>_<custom>", e.g.
  STAGING_MARTS. Override it so +schema: marts lands in DATAPULSE.MARTS --
  the schemas already created by warehouse/schema.sql.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
