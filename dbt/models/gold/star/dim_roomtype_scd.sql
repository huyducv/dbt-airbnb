{{ 
    config(
        materialized='table'
    ) 
}}

WITH s AS (
  SELECT
    room_type,
    first_seen_ts,
    snapshot_ts AS dbt_valid_from,
    LEAST(lead(snapshot_ts) over (partition by room_type order by snapshot_ts),
          timestamp '9999-12-31') AS dbt_valid_to
  FROM {{ ref('roomtype_snapshot') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['room_type','dbt_valid_from']) }} AS roomtype_sk,
  room_type,
  first_seen_ts,
  dbt_valid_from,
  dbt_valid_to
FROM s
