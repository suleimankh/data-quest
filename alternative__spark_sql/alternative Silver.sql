-- Databricks notebook source
-- =========================================================
-- SILVER SQL ALTERNATIVES
-- =========================================================

-- Data USA Silver

CREATE OR REFRESH STREAMING TABLE silver_population_sql
(
  CONSTRAINT valid_nation_id EXPECT (nation_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_year EXPECT (year IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_population EXPECT (population IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'SQL alternative: Clean annual US population data'
AS
SELECT DISTINCT
  record.`Nation ID` AS nation_id,
  record.Nation AS nation,
  CAST(record.Year AS INT) AS year,
  CAST(record.Population AS BIGINT) AS population
FROM (
  SELECT
    explode(data) AS record
  FROM STREAM bronze_population_sql
);


-- BLS observations

CREATE OR REFRESH STREAMING TABLE silver_bls_data_sql
(
  CONSTRAINT valid_series_id EXPECT (series_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_year EXPECT (year IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_period EXPECT (period IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_value EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'SQL alternative: Clean BLS productivity observations'
AS
SELECT DISTINCT
  TRIM(series_id) AS series_id,
  CAST(year AS INT) AS year,
  TRIM(period) AS period,
  CAST(value AS DOUBLE) AS value,
  TRIM(footnote_codes) AS footnote_codes
FROM STREAM bronze_bls_data_sql;


-- BLS series metadata

CREATE OR REFRESH STREAMING TABLE silver_bls_series_sql
(
  CONSTRAINT valid_series_id EXPECT (series_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'SQL alternative: Clean BLS series metadata'
AS
SELECT DISTINCT
  TRIM(series_id) AS series_id,
  TRIM(sector_code) AS sector_code,
  TRIM(measure_code) AS measure_code
FROM STREAM bronze_bls_series_sql;


-- BLS measure lookup

CREATE OR REFRESH STREAMING TABLE silver_bls_measure_sql
(
  CONSTRAINT valid_measure_code EXPECT (measure_code IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'SQL alternative: Clean BLS measure lookup'
AS
SELECT DISTINCT
  TRIM(measure_code) AS measure_code,
  TRIM(measure_text) AS measure_text
FROM STREAM bronze_bls_measure_sql;


-- BLS sector lookup

CREATE OR REFRESH STREAMING TABLE silver_bls_sector_sql
(
  CONSTRAINT valid_sector_code EXPECT (sector_code IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'SQL alternative: Clean BLS sector lookup'
AS
SELECT DISTINCT
  TRIM(sector_code) AS sector_code,
  TRIM(sector_name) AS sector_name
FROM STREAM bronze_bls_sector_sql;