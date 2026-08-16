from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    avg,
    stddev_pop,
    sum as spark_sum,
    row_number,
    concat_ws
)


# =========================================================
# QUESTION 1
# Mean and standard deviation of US population, 2013-2018
# =========================================================

@dp.materialized_view(
    comment="Gold: Mean and standard deviation of annual US population from 2013 through 2018"
)
def gold_population_stats():

    population = spark.read.table(
        "rearc.datausa.silver_population"
    )

    return (
        population
        .filter(
            (col("nation") == "United States") &
            col("year").between(2013, 2018)
        )
        .agg(
            avg("population").alias("mean_population"),
            stddev_pop("population").alias("stddev_population")
        )
    )


# =========================================================
# QUESTION 2
# Best year for every BLS series
# =========================================================

@dp.materialized_view(
    comment="Gold: Best year for every BLS series based on the largest sum of quarterly values"
)
def gold_best_year_by_series():

    bls = spark.read.table(
        "rearc.bls.silver_bls_data"
    )

    yearly = (
        bls
        .filter(
            col("period").isin("Q01", "Q02", "Q03", "Q04")
        )
        .groupBy(
            "series_id",
            "year"
        )
        .agg(
            spark_sum("value").alias("annual_value")
        )
    )

    ranking = (
        Window
        .partitionBy("series_id")
        .orderBy(
            col("annual_value").desc(),
            col("year").asc()
        )
    )

    best_year = (
        yearly
        .withColumn(
            "row_num",
            row_number().over(ranking)
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    series = spark.read.table(
        "rearc.bls.silver_bls_series"
    )

    measures = spark.read.table(
        "rearc.bls.silver_bls_measure"
    )

    sectors = spark.read.table(
        "rearc.bls.silver_bls_sector"
    )

    metadata = (
        series
        .join(
            measures,
            on="measure_code",
            how="left"
        )
        .join(
            sectors,
            on="sector_code",
            how="left"
        )
        .withColumn(
            "series_label",
            concat_ws(
                " - ",
                col("sector_name"),
                col("measure_text")
            )
        )
        .select(
            "series_id",
            "series_label"
        )
    )

    return (
        best_year
        .join(
            metadata,
            on="series_id",
            how="left"
        )
        .select(
            "series_id",
            "series_label",
            col("year").alias("best_year"),
            col("annual_value").alias("best_year_value")
        )
    )


# =========================================================
# QUESTION 3
# PRS30006032 / Q01 joined with US population
# =========================================================

@dp.materialized_view(
    comment="Gold: PRS30006032 Q01 values by year joined with annual US population"
)
def gold_series_population():

    bls = (
        spark.read.table(
            "rearc.bls.silver_bls_data"
        )
        .filter(
            (col("series_id") == "PRS30006032") &
            (col("period") == "Q01")
        )
        .select(
            "year",
            "value"
        )
    )

    population = (
        spark.read.table(
            "rearc.datausa.silver_population"
        )
        .filter(
            col("nation") == "United States"
        )
        .select(
            "year",
            "population"
        )
    )

    return (
        bls
        .join(
            population,
            on="year",
            how="left"
        )
        .select(
            "year",
            "value",
            "population"
        )
    )