{{ 
    config(
        materialized='table'
    ) 
}}

SELECT DISTINCT
  room_type,
  MIN(valid_from) AS first_seen_ts,
  MAX(valid_from) AS snapshot_ts
FROM {{ ref('listings_clean') }}
WHERE room_type IS NOT NULL
GROUP BY 1