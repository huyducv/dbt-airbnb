{{ 
    config(
        materialized='view'
) 
}}


WITH 
base AS (
  SELECT
    f.host_neighbourhood,
    f.month_start,
    f.host_id,
    f.has_availability,
    f.est_revenue
  FROM {{ ref('fact_listings_monthly_scd') }} f
  WHERE f.host_neighbourhood IS NOT NULL
),

host_lga AS(
    SELECT 
        b.*,
        b.host_neighbourhood,
        l.host_neighbourhood,
        l.lga_name          AS host_neighbourhood_lga
    FROM base b
        LEFT JOIN {{ref('dim_location_scd')}} l
        ON LOWER(b.host_neighbourhood) = LOWER(l.host_neighbourhood)
    WHERE l.lga_name IS NOT NULL
),

agg AS (
  SELECT
    host_neighbourhood_lga,
    month_start,

    -- Number of distinct hosts
    COUNT(DISTINCT host_id)         AS distinct_hosts,

    -- Average estimated revenue per active listing
    ROUND(AVG(est_revenue) FILTER (WHERE has_availability), 2)      AS avg_est_revenue_active,

    -- Estimated revenue per host (total revenue divided by distinct hosts)
    ROUND(SUM(est_revenue) / NULLIF(COUNT(DISTINCT host_id), 0), 2) AS est_revenue_per_host
  FROM host_lga
  GROUP BY host_neighbourhood_lga, month_start
)

SELECT *
FROM agg
ORDER BY host_neighbourhood_lga, month_start
