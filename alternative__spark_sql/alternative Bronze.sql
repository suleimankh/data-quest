-- Databricks notebook source
-- =========================================================
-- BRONZE SQL ALTERNATIVES
-- =========================================================

-- Data USA Bronze

CREATE OR REFRESH STREAMING TABLE bronze_population_sql
COMMENT 'SQL alternative: Raw DataUSA population snapshots'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
)
AS
SELECT
  *,
  _metadata
FROM STREAM read_files(
  '/Volumes/rearc/datausa/raw/',
  format => 'json',
  inferColumnTypes => true
);


-- BLS observations

CREATE OR REFRESH STREAMING TABLE bronze_bls_data_sql
COMMENT 'SQL alternative: Raw BLS productivity observations'
AS
SELECT
  *
FROM STREAM read_files(
  '/Volumes/rearc/bls/raw/',
  format => 'csv',
  header => true,
  sep => '\t',
  pathGlobFilter => 'pr.data.1.AllData*'
);


-- BLS series metadata

CREATE OR REFRESH STREAMING TABLE bronze_bls_series_sql
COMMENT 'SQL alternative: Raw BLS series metadata'
AS
SELECT
  *
FROM STREAM read_files(
  '/Volumes/rearc/bls/raw/',
  format => 'csv',
  header => true,
  sep => '\t',
  pathGlobFilter => 'pr.series*'
);


-- BLS measure lookup

CREATE OR REFRESH STREAMING TABLE bronze_bls_measure_sql
COMMENT 'SQL alternative: Raw BLS measure metadata'
AS
SELECT
  *
FROM STREAM read_files(
  '/Volumes/rearc/bls/raw/',
  format => 'csv',
  header => true,
  sep => '\t',
  pathGlobFilter => 'pr.measure*'
);


-- BLS sector lookup

CREATE OR REFRESH STREAMING TABLE bronze_bls_sector_sql
COMMENT 'SQL alternative: Raw BLS sector metadata'
AS
SELECT
  *
FROM STREAM read_files(
  '/Volumes/rearc/bls/raw/',
  format => 'csv',
  header => true,
  sep => '\t',
  pathGlobFilter => 'pr.sector*'
);