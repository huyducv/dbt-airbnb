{{
  config(
    materialized='table',
    alias='census_g01_bronze',
    unique_key='lga_code'
  )
}}

select
  *
from {{ source('bronze','census_g01_raw') }}
