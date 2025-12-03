{{
  config(
    materialized='table',
    alias='nsw_lga_code_bronze',
    unique_key='lga_code'
  )
}}

select
  *
from {{ source('bronze','nsw_lga_code_raw') }}
