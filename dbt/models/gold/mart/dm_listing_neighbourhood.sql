{{ 
    config(
        materialized='view'
        ) 
}}

WITH base AS (
  SELECT
    f.listing_neighbourhood,
    f.month_start,
    f.host_id,
    f.price,
    f.has_availability,
    f.review_scores_rating,
    f.stays_in_month,
    f.est_revenue,
    h.host_is_superhost
  FROM {{ ref('fact_listings_monthly_scd') }} f
  LEFT JOIN {{ ref('dim_host_scd') }} h
    ON f.host_sk = h.host_sk
),


agg AS (
  SELECT
    listing_neighbourhood,
    month_start,

    -- Active listing metrics
    COUNT(*)                                                        AS total_listings,
    SUM(CASE WHEN has_availability THEN 1 ELSE 0 END)               AS active_listings,
    ROUND(SUM(CASE WHEN has_availability THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)             AS active_listings_rate,

    -- Price metrics (for active listings)
    MIN(price) FILTER (WHERE has_availability)                      AS min_price,
    MAX(price) FILTER (WHERE has_availability)                      AS max_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE has_availability)      AS median_price,
    AVG(price) FILTER (WHERE has_availability)                      AS avg_price,

    -- Distinct hosts and superhost rate
    COUNT(DISTINCT host_id)                                         AS distinct_hosts,
    ROUND(SUM(CASE WHEN host_is_superhost THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT host_id), 0) * 100, 2)            AS superhost_rate,

    -- Avg review score (for active listings)
    ROUND(AVG(NULLIF(review_scores_rating, 'nan'::numeric)) FILTER (WHERE has_availability), 2)                        AS avg_review_score,

    -- Total stays and avg estimated revenue
    SUM(stays_in_month) FILTER (WHERE has_availability)             AS total_stays,
    ROUND(AVG(est_revenue) FILTER (WHERE has_availability), 2)      AS avg_est_revenue_active
  FROM base
  GROUP BY listing_neighbourhood, month_start
),

pct_change AS (
  SELECT
    a.*,
    ROUND(
      100.0 * (a.active_listings - LAG(a.active_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start))
      / NULLIF(LAG(a.active_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start), 0), 2
    ) AS pct_change_active,
    ROUND(
      100.0 * ((a.total_listings - a.active_listings) - 
      (LAG(a.total_listings - a.active_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start)))
      / NULLIF(LAG(a.total_listings - a.active_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start), 0), 2
    ) AS pct_change_inactive
  FROM agg a
)

SELECT *
FROM pct_change
ORDER BY listing_neighbourhood, month_start
