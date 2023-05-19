{%- macro unnest_key_with_null_default(column_to_unnest, key_to_extract, value_type = "string_value") -%}
(select 
    case when key = '{{key_to_extract}}' then value.{{value_type}} 
    else null
    end
from unnest({{column_to_unnest}}) 
where key = '{{key_to_extract}}')
{%- endmacro -%}