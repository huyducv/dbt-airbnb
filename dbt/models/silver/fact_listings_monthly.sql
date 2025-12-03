{{ 
    config(
        materialized='table'
    ) 
}}

-- grain: listing_id x month_start
WITH 
base AS (
  SELECT
    listing_id,
    date_trunc('month', scraped_date)::date     AS month_start,
    has_availability,
    availability_30,
    price,
    host_id,
    listing_neighbourhood,
    host_neighbourhood,
    property_type,
    room_type, 
    review_scores_rating, 
    accommodates
  FROM {{ ref('listings_clean') }}
)
SELECT
  listing_id,
  month_start,
  host_id,
  listing_neighbourhood,
  host_neighbourhood,
  property_type,
  room_type,
  has_availability,
  availability_30,
  price,
  review_scores_rating,
  accommodates,
  (30 - COALESCE(availability_30,0))::int   AS stays_in_month,    
  CASE WHEN has_availability THEN price * (30 - COALESCE(availability_30,0))
       ELSE 0 END::numeric AS est_revenue
FROM base
