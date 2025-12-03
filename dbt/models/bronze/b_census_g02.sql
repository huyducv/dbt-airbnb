{{
  config(
    materialized='table',
    alias='census_g02_bronze',
    unique_key='lga_code'
  )
}}

select
  *
from {{ source('bronze','census_g02_raw') }}
