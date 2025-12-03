{{ 
    config(
        materialized='table'
    ) 
}}

-- map listing_neighbourhood -> LGA
WITH lga_suburbs AS (
  SELECT DISTINCT
    suburb_name,
    lga_name
  FROM {{ ref('b_nsw_lga_suburb') }}
  WHERE suburb_name IS NOT NULL
),

lga_codes AS (
    SELECT DISTINCT
        lga_name,
        lga_code
    FROM {{ ref('b_nsw_lga_code') }}
    WHERE lga_name IS NOT NULL
),

joined AS (
    SELECT DISTINCT
        s.host_neighbourhood,
        ls.lga_name,
        ls.suburb_name,
        lc.lga_code,
        MAX(s.valid_from) AS snapshot_ts
    FROM {{ ref('listings_clean') }} s
    LEFT JOIN lga_suburbs ls
        ON LOWER(s.host_neighbourhood) = LOWER(ls.suburb_name)
    LEFT JOIN lga_codes lc
        ON LOWER(ls.lga_name) = LOWER(lc.lga_name)
    GROUP BY 1, 2, 3, 4
    HAVING host_neighbourhood IS NOT NULL
)

SELECT * FROM joined