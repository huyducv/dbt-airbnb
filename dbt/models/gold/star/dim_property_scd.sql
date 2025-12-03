{{ 
    config(
        materialized='table'
        ) 
}}

WITH s AS (
  SELECT
    property_type,
    first_seen_ts,
    snapshot_ts AS dbt_valid_from,
    LEAST(lead(snapshot_ts) over (partition by property_type order by snapshot_ts),
          timestamp '9999-12-31') AS dbt_valid_to
  FROM {{ ref('property_snapshot') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['property_type','dbt_valid_from']) }} AS property_sk,
  property_type,
  first_seen_ts,
  dbt_valid_from,
  dbt_valid_to
FROM s
