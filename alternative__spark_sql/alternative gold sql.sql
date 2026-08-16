-- Databricks notebook source
-- =========================================================
-- Rearc Data Quest
-- Spark SQL alternatives to the PySpark Gold implementation
-- =========================================================


-- =========================================================
-- QUESTION 1
-- Mean and standard deviation of US population
-- 2013-2018 inclusive
-- =========================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_population_stats_sql
COMMENT 'SQL alternative: Mean and standard deviation of annual US population from 2013 through 2018'
AS
SELECT
    AVG(population) AS mean_population,
    STDDEV_POP(population) AS stddev_population
FROM rearc.datausa.silver_population
WHERE nation = 'United States'
  AND year BETWEEN 2013 AND 2018;


-- =========================================================
-- QUESTION 2
-- Best year for each BLS series
-- =========================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_best_year_by_series_sql
COMMENT 'SQL alternative: Best year for every BLS series based on quarterly annual sum'
AS

WITH yearly_values AS (

    SELECT
        series_id,
        year,
        SUM(value) AS annual_value
    FROM rearc.bls.silver_bls_data
    WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
    GROUP BY
        series_id,
        year
),

ranked_years AS (

    SELECT
        series_id,
        year,
        annual_value,

        ROW_NUMBER() OVER (
            PARTITION BY series_id
            ORDER BY annual_value DESC, year ASC
        ) AS row_num

    FROM yearly_values
),

series_metadata AS (

    SELECT
        s.series_id,

        CONCAT_WS(
            ' - ',
            sec.sector_name,
            m.measure_text
        ) AS series_label

    FROM rearc.bls.silver_bls_series s

    LEFT JOIN rearc.bls.silver_bls_measure m
        ON s.measure_code = m.measure_code

    LEFT JOIN rearc.bls.silver_bls_sector sec
        ON s.sector_code = sec.sector_code
)

SELECT
    r.series_id,
    m.series_label,
    r.year AS best_year,
    r.annual_value AS best_year_value

FROM ranked_years r

LEFT JOIN series_metadata m
    ON r.series_id = m.series_id

WHERE r.row_num = 1;


-- =========================================================
-- QUESTION 3
-- PRS30006032 / Q01 joined with population
-- =========================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_series_population_sql
COMMENT 'SQL alternative: PRS30006032 Q01 by year joined with annual US population'
AS

SELECT
    b.year,
    b.value,
    p.population

FROM rearc.bls.silver_bls_data b

LEFT JOIN rearc.datausa.silver_population p
    ON b.year = p.year
   AND p.nation = 'United States'

WHERE b.series_id = 'PRS30006032'
  AND b.period = 'Q01';