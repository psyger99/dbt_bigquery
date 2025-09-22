-- Macro: count_events_by_name
-- Parameters:
--   event_names: list of event names to pivot/count
--   field: column name containing event names (default: "event_name")
--   timestamp: column name containing event timestamps (default: "event_timestamp")
-- Usage:
--   Generates SQL to count distinct events by name, pivoted as columns.

-- jinja loop for pivoting out event names' values:
{%- macro count_events_by_name(event_names, field="event_name", timestamp="event_timestamp") -%}
    {%- for event_name in event_names %}
    count(distinct case when {{ field }} = '{{ event_name }}' then {{ timestamp }} end) as num_{{ event_name }}_events
    {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
{%- endmacro %}

-- Macro: browser_alias
-- Generates a safe alias for browser names
{%- macro browser_alias(browser) -%}
    {{ browser
        | lower
        | replace("<other>", "other")
        | replace(" ", "_")
        | replace("<", "")
        | replace(">", "")
        | replace("-", "_")
        | replace(".", "_")
        | replace("/", "_")
    }}
{%- endmacro %}

-- Macro: count_by_browser
-- Parameters:
--   browsers: list of browser names to pivot
--   browser_field: field name containing browser values (default: "user_device_browser")
--   timestamp: field name containing event timestamps (default: "event_timestamp")
-- Usage:
--   Use in a select statement to generate counts for each browser in the list

-- jinja loop for pivoting out browsers' values:
{%- macro count_by_browser(browsers, browser_field="user_device_browser", timestamp="event_timestamp") -%}
    {%- for browser in browsers %}
    count(distinct case when {{ browser_field }} = '{{ browser }}' then {{ timestamp }} end) as num_browser_{{ browser_alias(browser) }}
    {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
{%- endmacro %}

