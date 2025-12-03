--a. What are the demographic differences (e.g., age group distribution, household size) between the top 3 performing and lowest 3 performing LGAs based on estimated revenue per active listing over the last 12 months?

WITH lga_perf AS (
    SELECT
        host_neighbourhood_lga AS lga_name,
        AVG(avg_est_revenue_active) AS avg_revenue_per_active
    FROM dbt_hvu_gold.dm_host_neighbourhood
    WHERE month_start BETWEEN DATE '2020-05-01' AND DATE '2021-04-30'
    GROUP BY 1
),

-- Rank LGAs
ranked AS (
    SELECT
        lga_name,
        avg_revenue_per_active,
        RANK() OVER (ORDER BY avg_revenue_per_active DESC) AS rev_rank_desc,
        RANK() OVER (ORDER BY avg_revenue_per_active ASC)  AS rev_rank_asc
    FROM lga_perf
),

-- Select Top 3 & Bottom 3 performing LGAs
top_bottom AS (
    SELECT lga_name, avg_revenue_per_active,
           CASE
             WHEN rev_rank_desc <= 3 THEN 'Top 3'
             WHEN rev_rank_asc  <= 3 THEN 'Bottom 3'
           END AS performance_group
    FROM ranked
    WHERE rev_rank_desc <= 3 OR rev_rank_asc <= 3
),

lga_map AS (
    SELECT
        UPPER(lga_name) AS lga_name,
        lga_code       AS lga_code
    FROM dbt_hvu_bronze.nsw_lga_code_bronze
),

g01 AS (
    SELECT
        LTRIM(lga_code, 'LGA') AS lga_code,
        (payload->>'Age_25_34_yr_P')::NUMERIC 			AS age_25_34_pct
    FROM dbt_hvu_bronze.census_g01_bronze
),

g02 AS (
    SELECT
        LTRIM(lga_code, 'LGA') AS lga_code,
        (payload->>'Average_household_size')::NUMERIC                  AS avg_household_size,
        (payload->>'Median_age_persons')::NUMERIC                      AS median_age
    FROM dbt_hvu_bronze.census_g02_bronze
),

joined AS (
    SELECT
        tb.performance_group,
        tb.lga_name,
        tb.avg_revenue_per_active,
        g02.median_age,
        g01.age_25_34_pct,
        g02.avg_household_size
    FROM top_bottom tb
    LEFT JOIN lga_map m
      ON UPPER(tb.lga_name) = m.lga_name   
    LEFT JOIN g01
      ON g01.lga_code = m.lga_code                     
    LEFT JOIN g02
      ON g02.lga_code = m.lga_code                     
)

SELECT
    performance_group,
    lga_name,
    ROUND(avg_revenue_per_active, 2) AS avg_revenue_per_active,
    ROUND(median_age, 1)             AS median_age,
    ROUND(age_25_34_pct, 1)          AS pct_age_25_34,
    ROUND(avg_household_size, 2)     AS avg_household_size
FROM joined
ORDER BY performance_group DESC, avg_revenue_per_active DESC;


--b. Is there a correlation between the median age of a neighbourhood (from Census data) and the revenue generated per active listing in that neighbourhood?

-- Correlation data points
WITH rev_by_neigh AS (
    SELECT
        listing_neighbourhood,
        AVG(avg_est_revenue_active) AS avg_rev_active
    FROM dbt_hvu_gold.dm_listing_neighbourhood
    GROUP BY 1
),
suburb_to_lga AS (
    SELECT LOWER(suburb_name) AS suburb_lc, UPPER(lga_name) AS lga_name
    FROM dbt_hvu_bronze.nsw_lga_suburb_bronze
),
lga_name_to_code AS (
    SELECT UPPER(lga_name) AS lga_name, lga_code
    FROM dbt_hvu_bronze.nsw_lga_code_bronze
),
census_age AS (
    SELECT LTRIM(lga_code, 'LGA') AS lga_code,
           (payload->>'Median_age_persons')::NUMERIC AS median_age
    FROM dbt_hvu_bronze.census_g02_bronze
)
SELECT
    r.listing_neighbourhood,
    n.lga_code,
    ca.median_age,
    r.avg_rev_active
FROM rev_by_neigh r
LEFT JOIN suburb_to_lga s
  ON LOWER(r.listing_neighbourhood) = s.suburb_lc
LEFT JOIN lga_name_to_code n
  ON s.lga_name = n.lga_name
LEFT JOIN census_age ca
  ON n.lga_code = ca.lga_code
WHERE ca.median_age IS NOT NULL
  AND r.avg_rev_active IS NOT NULL
ORDER BY r.avg_rev_active DESC;


-- Correlation score
WITH rev_by_neigh AS (
    SELECT
        listing_neighbourhood,
        AVG(avg_est_revenue_active) AS avg_rev_active
    FROM dbt_hvu_gold.dm_listing_neighbourhood
    GROUP BY 1
),

suburb_to_lga AS (
    SELECT
        LOWER(suburb_name) AS suburb_lc,
        UPPER(lga_name)     AS lga_name
    FROM dbt_hvu_bronze.nsw_lga_suburb_bronze
),

lga_name_to_code AS (
    SELECT
        UPPER(lga_name) AS lga_name,
        lga_code
    FROM dbt_hvu_bronze.nsw_lga_code_bronze
),

census_age AS (
    SELECT
        LTRIM(lga_code, 'LGA')                    AS lga_code,
        (payload->>'Median_age_persons')::NUMERIC AS median_age
    FROM dbt_hvu_bronze.census_g02_bronze
),

neigh_with_age AS (
    SELECT
        r.listing_neighbourhood,
        r.avg_rev_active,
        ca.median_age
    FROM rev_by_neigh r
    LEFT JOIN suburb_to_lga s
      ON LOWER(r.listing_neighbourhood) = s.suburb_lc
    LEFT JOIN lga_name_to_code n
      ON s.lga_name = n.lga_name
    LEFT JOIN census_age ca
      ON n.lga_code = ca.lga_code
    WHERE ca.median_age IS NOT NULL
      AND r.avg_rev_active IS NOT NULL
)

SELECT
    COUNT(*)                                        AS n_neighbourhoods,
    ROUND(CORR(avg_rev_active, median_age)::NUMERIC, 4) AS pearson_corr,
    ROUND(REGR_SLOPE(avg_rev_active, median_age)::NUMERIC, 4)   AS slope_rev_per_year,
    ROUND(REGR_INTERCEPT(avg_rev_active, median_age)::NUMERIC, 2) AS intercept
FROM neigh_with_age;


--c. What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of average estimated revenue per active listing) to have the highest number of stays?
WITH top5_neigh AS (
    SELECT
        listing_neighbourhood
    FROM (
        SELECT
            listing_neighbourhood,
            AVG(avg_est_revenue_active) AS avg_rev_active
        FROM dbt_hvu_gold.dm_listing_neighbourhood
        GROUP BY 1
    ) x
    ORDER BY avg_rev_active DESC
    LIMIT 5
),

-- Compute avg stays & revenue by (neighbourhood, property_type, room_type, accommodates)
combo_stats AS (
    SELECT
        UPPER(l.listing_neighbourhood) AS listing_neighbourhood,
        l.property_type,
        l.room_type,
        l.accommodates,
        -- average number of stays across the window, active listings only
        AVG(CASE WHEN l.has_availability
                 THEN (30 - COALESCE(l.availability_30, 0))::numeric
            END) AS avg_stays_active,
        -- tie-breaker: higher avg revenue among active listings
        AVG(CASE WHEN l.has_availability
                 THEN (l.price * (30 - COALESCE(l.availability_30, 0)))::numeric
            END) AS avg_revenue_active
    FROM dbt_hvu_gold.fact_listings_monthly_scd l
    JOIN top5_neigh t
      ON UPPER(l.listing_neighbourhood) = UPPER(t.listing_neighbourhood)
    WHERE l.month_start BETWEEN DATE '2020-05-01' AND DATE '2021-04-30'
    GROUP BY 1,2,3,4
),

ranked AS (
    SELECT
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        ROUND(avg_stays_active, 2)   AS avg_stays_active,
        ROUND(avg_revenue_active, 2) AS avg_revenue_active,
        RANK() OVER (
            PARTITION BY listing_neighbourhood
            ORDER BY avg_stays_active DESC, avg_revenue_active DESC
        ) AS rnk
    FROM combo_stats
)

-- Best combo per neighbourhood
SELECT
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    avg_stays_active,
    avg_revenue_active
FROM ranked
WHERE rnk = 1
ORDER BY avg_revenue_active DESC;

--d. For hosts with multiple listings are their properties concentrated within the same LGA, or are they distributed across different LGAs?

--list
WITH base AS (
  SELECT DISTINCT host_id, listing_id, UPPER(listing_neighbourhood) AS lga_name
  FROM dbt_hvu_silver.fact_listings_monthly
  
),
host_multi AS (
  SELECT
    host_id,
    COUNT(DISTINCT listing_id) AS n_listings,
    COUNT(DISTINCT lga_name)   AS n_lgas
  FROM base
  GROUP BY 1
  HAVING COUNT(DISTINCT listing_id) >= 2
)
SELECT
  host_id,
  n_listings,
  n_lgas
FROM host_multi
ORDER BY n_lgas DESC, n_listings DESC;

--count
WITH base AS (
  SELECT DISTINCT host_id, listing_id, UPPER(listing_neighbourhood) AS lga_name
  FROM dbt_hvu_silver.fact_listings_monthly
  
),
host_multi AS (
  SELECT
    host_id,
    COUNT(DISTINCT listing_id) AS n_listings,
    COUNT(DISTINCT lga_name)   AS n_lgas
  FROM base
  GROUP BY 1
  HAVING COUNT(DISTINCT listing_id) >= 2
)
SELECT
  SUM(case when n_lgas = 1 then 1 end) as superhosts_single_lga,
  SUM(case when n_lgas != 1 then 1 end) as superhosts_multiple_lga
FROM host_multi;


--e. For hosts with a single Airbnb listing does the estimated revenue over the last 12 months cover the annualised median mortgage repayment in the corresponding LGA? Which LGA has the highest percentage of hosts that can cover it?

WITH base AS (
  SELECT DISTINCT
      host_id,
      listing_id
  FROM dbt_hvu_silver.fact_listings_monthly
  WHERE month_start BETWEEN DATE '2020-05-01' AND DATE '2021-04-30'
)
SELECT
    COUNT(*) AS single_listing_hosts
FROM (
    SELECT
        host_id,
        COUNT(DISTINCT listing_id) AS n_listings
    FROM base
    GROUP BY host_id
    HAVING COUNT(DISTINCT listing_id) = 1
) sub;


WITH single_hosts AS (
  SELECT
      host_id,
      COUNT(DISTINCT listing_id) AS n_listings,
      UPPER(listing_neighbourhood) AS lga_name
  FROM dbt_hvu_silver.fact_listings_monthly
  WHERE month_start BETWEEN DATE '2020-05-01' AND DATE '2021-04-30'
  GROUP BY 1, 3
  HAVING COUNT(DISTINCT listing_id) = 1
),

-- 12-month estimated revenue per single-listing host
host_revenue AS (
  SELECT
      f.host_id,
      UPPER(f.listing_neighbourhood) AS lga_name,
      SUM(f.est_revenue) AS total_revenue_12m
  FROM dbt_hvu_silver.fact_listings_monthly f
  JOIN single_hosts s
    ON f.host_id = s.host_id
   AND UPPER(f.listing_neighbourhood) = s.lga_name
  WHERE f.month_start BETWEEN DATE '2020-05-01' AND DATE '2021-04-30'
  GROUP BY 1,2
),

-- LGA name -> LGA code
lga_map AS (
  SELECT 
  	UPPER(lga_name) AS lga_name, 
  	lga_code
  FROM dbt_hvu_bronze.nsw_lga_code_bronze
),

-- Mortgage repayment data from Census G02 (monthly -> annual)
census_mortgage AS (
  SELECT
      LTRIM(lga_code, 'LGA') AS lga_code,
      (payload->>'Median_mortgage_repay_monthly')::NUMERIC AS median_mortgage_monthly,
      ((payload->>'Median_mortgage_repay_monthly')::NUMERIC * 12) AS annual_mortgage
  FROM dbt_hvu_bronze.census_g02_bronze
),

-- Combine host revenue + mortgage by LGA
combined AS (
  SELECT
      hr.host_id,
      hr.lga_name,
      hr.total_revenue_12m,
      cm.annual_mortgage
  FROM host_revenue hr
  LEFT JOIN lga_map m
    ON hr.lga_name = m.lga_name
  LEFT JOIN census_mortgage cm
    ON m.lga_code = cm.lga_code
  WHERE cm.annual_mortgage IS NOT NULL
),

-- flag whether host covers mortgage
flagged AS (
  SELECT
      lga_name,
      host_id,
      total_revenue_12m,
      annual_mortgage,
      CASE WHEN total_revenue_12m >= annual_mortgage THEN 1 ELSE 0 END AS covers_mortgage
  FROM combined
)

-- final LGA-level summary
SELECT
    lga_name,
    COUNT(DISTINCT host_id) AS total_hosts,
    SUM(covers_mortgage)    AS hosts_covering_mortgage,
    ROUND(100.0 * SUM(covers_mortgage) / COUNT(DISTINCT host_id), 2) AS pct_hosts_covering,
    ROUND(AVG(total_revenue_12m), 0)    AS avg_host_revenue,
    ROUND(AVG(annual_mortgage), 0)      AS avg_annual_mortgage
FROM flagged
GROUP BY lga_name
ORDER BY pct_hosts_covering DESC;

