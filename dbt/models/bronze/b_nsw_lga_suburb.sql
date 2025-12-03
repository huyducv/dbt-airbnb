{{
  config(
    materialized='table',
    alias='nsw_lga_suburb_bronze',
    unique_key='suburb_sk'
  )
}}

select
  {{ dbt_utils.generate_surrogate_key(['lga_name','suburb_name']) }} as suburb_sk,
  *
from {{ source('bronze','nsw_lga_suburb_raw') }}
