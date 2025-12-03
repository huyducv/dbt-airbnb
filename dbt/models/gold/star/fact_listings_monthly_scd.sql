{{ 
    config(
        materialized='table'
) 
}}



WITH f AS (
  SELECT
    listing_id,
    month_start::date AS month_start,
    host_id,
    listing_neighbourhood,
    accommodates,
    LOWER(TRIM(host_neighbourhood))  AS host_neighbourhood_lc,
    LOWER(TRIM(property_type))       AS property_type_lc,
    LOWER(TRIM(room_type))           AS room_type_lc,

    has_availability::boolean        AS has_availability,
    availability_30::int             AS availability_30,
    price::numeric                   AS price,
    review_scores_rating::numeric    AS review_scores_rating,

    (30 - COALESCE(availability_30,0))::int AS stays_in_month,
    CASE WHEN has_availability
         THEN price * (30 - COALESCE(availability_30,0))
         ELSE 0 END::numeric                AS est_revenue
  FROM {{ ref('fact_listings_monthly') }}
),

-- Normalize & coalesce valid_to once for each SCD dim
h_scd AS (
  SELECT host_id, host_sk,
         dbt_valid_from::date AS valid_from,
         COALESCE(dbt_valid_to::date, '9999-12-31'::date) AS valid_to
  FROM {{ ref('dim_host_scd') }}
),
l_scd AS (
  SELECT LOWER(TRIM(host_neighbourhood)) AS host_neighbourhood_lc,
         location_sk,
         dbt_valid_from::date AS valid_from,
         COALESCE(dbt_valid_to::date, '9999-12-31'::date) AS valid_to
  FROM {{ ref('dim_location_scd') }}
),
p_scd AS (
  SELECT LOWER(TRIM(property_type)) AS property_type_lc,
         property_sk,
         dbt_valid_from::date AS valid_from,
         COALESCE(dbt_valid_to::date, '9999-12-31'::date) AS valid_to
  FROM {{ ref('dim_property_scd') }}
),
r_scd AS (
  SELECT LOWER(TRIM(room_type)) AS room_type_lc,
         roomtype_sk,
         dbt_valid_from::date AS valid_from,
         COALESCE(dbt_valid_to::date, '9999-12-31'::date) AS valid_to
  FROM {{ ref('dim_roomtype_scd') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['f.listing_id','f.month_start']) }} AS listing_month_sk,
  f.month_start,

  -- SKs chosen by best-match logic (interval > latest prior > earliest)
  h.host_sk,
  l.location_sk,
  p.property_sk,
  r.roomtype_sk,

  -- IDs & natural keys to help marts
  f.host_id,
  f.listing_neighbourhood,
  f.host_neighbourhood_lc AS host_neighbourhood,
  f.property_type_lc      AS property_type,
  f.room_type_lc          AS room_type,

  -- metrics/flags
  f.has_availability,
  f.availability_30,
  f.price,
  f.review_scores_rating,
  f.stays_in_month,
  f.est_revenue,
  f.accommodates

FROM f

-- HOST (pick one row)
LEFT JOIN LATERAL (
  SELECT h1.host_sk
  FROM h_scd h1
  WHERE h1.host_id = f.host_id
  ORDER BY
    CASE WHEN f.month_start >= h1.valid_from AND f.month_start < h1.valid_to THEN 0 ELSE 1 END, --prefer latest prior if not in interval; if none, earliest overall
    CASE WHEN f.month_start >= h1.valid_from THEN (f.month_start - h1.valid_from) END DESC NULLS LAST,
    h1.valid_from ASC
  LIMIT 1
) h ON TRUE

-- LOCATION 
LEFT JOIN LATERAL (
  SELECT l1.location_sk
  FROM l_scd l1
  WHERE l1.host_neighbourhood_lc = f.host_neighbourhood_lc
  ORDER BY
    CASE WHEN f.month_start >= l1.valid_from AND f.month_start < l1.valid_to THEN 0 ELSE 1 END,
    CASE WHEN f.month_start >= l1.valid_from THEN (f.month_start - l1.valid_from) END DESC NULLS LAST,
    l1.valid_from ASC
  LIMIT 1
) l ON TRUE

-- PROPERTY TYPE
LEFT JOIN LATERAL (
  SELECT p1.property_sk
  FROM p_scd p1
  WHERE p1.property_type_lc = f.property_type_lc
  ORDER BY
    CASE WHEN f.month_start >= p1.valid_from AND f.month_start < p1.valid_to THEN 0 ELSE 1 END,
    CASE WHEN f.month_start >= p1.valid_from THEN (f.month_start - p1.valid_from) END DESC NULLS LAST,
    p1.valid_from ASC
  LIMIT 1
) p ON TRUE

-- ROOM TYPE
LEFT JOIN LATERAL (
  SELECT r1.roomtype_sk
  FROM r_scd r1
  WHERE r1.room_type_lc = f.room_type_lc
  ORDER BY
    CASE WHEN f.month_start >= r1.valid_from AND f.month_start < r1.valid_to THEN 0 ELSE 1 END,
    CASE WHEN f.month_start >= r1.valid_from THEN (f.month_start - r1.valid_from) END DESC NULLS LAST,
    r1.valid_from ASC
  LIMIT 1
) r ON TRUE
