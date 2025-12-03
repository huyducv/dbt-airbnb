{{ 
    config(
        materialized='table'
        ) 
}}

WITH s AS (
  SELECT
    host_neighbourhood,
    lga_name,
    lga_code,
    dbt_valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as dbt_valid_to
  FROM {{ ref('location_snapshot') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['host_neighbourhood','dbt_valid_from']) }} AS location_sk,
  host_neighbourhood,
  lga_name,
  lga_code,
  dbt_valid_from,
  dbt_valid_to
FROM s
