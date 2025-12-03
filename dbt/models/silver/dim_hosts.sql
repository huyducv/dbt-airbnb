{{ 
    config(
        materialized='table'
    ) 
}}

SELECT DISTINCT
  host_id,
  host_name,
  host_since,
  host_is_superhost,
  max(valid_from) AS snapshot_ts
FROM {{ ref('listings_clean') }}
WHERE host_id IS NOT NULL
GROUP BY 1, 2, 3, 4