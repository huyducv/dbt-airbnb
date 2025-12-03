{{
  config(
    materialized='table',
    alias='airbnb_listings_bronze',
    unique_key='listing_sk'
  )
}}

select
  {{ dbt_utils.generate_surrogate_key(['listing_id','scraped_date']) }} as listing_sk,
  *
from {{ source('bronze','airbnb_listings_raw') }}
