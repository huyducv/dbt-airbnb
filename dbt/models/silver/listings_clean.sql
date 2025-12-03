{{ 
    config(
        materialized='table'
    ) 
}}

WITH base AS (
  SELECT
    listing_id::bigint                 AS listing_id,
    scrape_id::bigint                  AS scrape_id,
    COALESCE(scraped_date::date, CURRENT_DATE) AS scraped_date,
    host_id::bigint                    AS host_id,
    host_name,
    host_since::date                   AS host_since,
    host_is_superhost::boolean         AS host_is_superhost,
    host_neighbourhood,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates::int                  AS accommodates,
    price::numeric                     AS price,
    has_availability::boolean          AS has_availability,
    availability_30::int               AS availability_30,
    number_of_reviews::int             AS number_of_reviews,
    review_scores_rating::numeric      AS review_scores_rating,
    review_scores_accuracy::numeric    AS review_scores_accuracy,
    review_scores_cleanliness::numeric AS review_scores_cleanliness,
    review_scores_checkin::numeric     AS review_scores_checkin,
    review_scores_communication::numeric AS review_scores_communication,
    review_scores_value::numeric       AS review_scores_value,
    source_file,
    scraped_date                       AS valid_from,         -- for SCD joins later
    date_trunc('month', scraped_date)  AS month_start
  FROM {{ ref('b_airbnb_listings') }}
)
SELECT * FROM base
