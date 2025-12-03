{{ 
    config(
        materialized='table'
    ) 
}}

WITH s AS (
  SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    dbt_valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as dbt_valid_to
  FROM {{ ref('host_snapshot') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['host_id','dbt_valid_from']) }} AS host_sk,
  host_id,
  host_name,
  host_since,
  host_is_superhost,
  dbt_valid_from,
  dbt_valid_to
FROM s
