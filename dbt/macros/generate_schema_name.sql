{#
  Use the schema a model asks for, verbatim.

  dbt's built-in behaviour concatenates the target schema onto the custom one,
  so a model asking for `hotspots` would land in `undeclared_hotspots`. That
  default exists to keep developers sharing one warehouse from overwriting each
  other, and it is the wrong trade here: these models are a published interface,
  and they are meant to sit in the same schema as the data they summarise --
  `hotspots.enabled_carriers_inventory` beside
  `hotspots.enabled_carriers_history`. A name that encodes whose target built it
  is not something a consumer can depend on, and it would not sit beside
  anything.

  Isolation is still available where it is actually needed: `DBT_SCHEMA_PREFIX`
  prefixes every schema, so a shared cluster can give each developer their own
  set (`brian_hotspots`) without changing what the deployed project writes.
  Local development needs none of it -- the docker-compose stack in this repo is
  per-machine and disposable.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set prefix = env_var('DBT_SCHEMA_PREFIX', '') -%}
    {%- set base = (custom_schema_name | default(target.schema, true)) | trim -%}
    {{ prefix ~ base }}
{%- endmacro %}
