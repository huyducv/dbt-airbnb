CREATE SCHEMA IF NOT EXISTS bronze;

-- 1. Airbnb raw
CREATE TABLE IF NOT EXISTS bronze.airbnb_listings_raw (
    listing_id                  BIGINT,
    scrape_id                   BIGINT,
    scraped_date                DATE,
    host_id                     BIGINT,
    host_name                   TEXT,
    host_since                  DATE,
    host_is_superhost           BOOLEAN,
    host_neighbourhood          TEXT,
    listing_neighbourhood       TEXT,
    property_type               TEXT,
    room_type                   TEXT,
    accommodates                INT,
    price                       NUMERIC,
    has_availability            BOOLEAN,
    availability_30             INT,
    number_of_reviews           INT,
    review_scores_rating        NUMERIC, 
    review_scores_accuracy      NUMERIC,
    review_scores_cleanliness   NUMERIC,
    review_scores_checkin       NUMERIC,
    review_scores_communication NUMERIC,
    review_scores_value         NUMERIC,
    source_file                 TEXT,
    loaded_at                   TIMESTAMP DEFAULT now()
);


-- 2. Census G01
CREATE TABLE IF NOT EXISTS bronze.census_g01_raw (
    lga_code      TEXT,
    payload       JSONB,
    source_file   TEXT,
    loaded_at     TIMESTAMP DEFAULT now()
);


-- 3. Census G02
CREATE TABLE IF NOT EXISTS bronze.census_g02_raw (
    lga_code      TEXT,
    payload       JSONB,
    source_file   TEXT,
    loaded_at     TIMESTAMP DEFAULT now()
);


-- 4. NSW_LGA_CODE
CREATE TABLE IF NOT EXISTS bronze.nsw_lga_code_raw (
    lga_name    TEXT,
    source_file TEXT,
    loaded_at   TIMESTAMP DEFAULT now()
);


-- 5. NSW_LGA_SUBURB
CREATE TABLE IF NOT EXISTS bronze.nsw_lga_suburb_raw (
    lga_name    TEXT,
    suburb_name TEXT,
    source_file TEXT,
    loaded_at   TIMESTAMP DEFAULT now()
);


SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'bronze';
