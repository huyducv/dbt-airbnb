{{ 
    config(
        materialized='table'
    ) 
}}

SELECT DISTINCT
  property_type,
  MIN(valid_from) AS first_seen_ts,
  MAX(valid_from) AS snapshot_ts
FROM {{ ref('listings_clean') }}
WHERE property_type IS NOT NULL
GROUP BY 1