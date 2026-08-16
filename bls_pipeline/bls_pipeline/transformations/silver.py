from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim


@dp.table(
    comment="Silver layer: Clean and validated BLS productivity observations"
)
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL")
@dp.expect_or_drop("valid_year", "year IS NOT NULL")
@dp.expect_or_drop("valid_period", "period IS NOT NULL")
@dp.expect_or_drop("valid_value", "value IS NOT NULL")
def silver_bls_data():

    return (
        spark.readStream.table("bronze_bls_data")
        .select(
            trim(col("series_id")).alias("series_id"),
            col("year").cast("int").alias("year"),
            trim(col("period")).alias("period"),
            col("value").cast("double").alias("value"),
            trim(col("footnote_codes")).alias("footnote_codes"),
            col("_metadata")
        )
        .dropDuplicates([
            "series_id",
            "year",
            "period"
        ])
    )


@dp.table(
    comment="Silver layer: Clean BLS series metadata"
)
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL")
def silver_bls_series():

    return (
        spark.readStream.table("bronze_bls_series")
        .select(
            trim(col("series_id")).alias("series_id"),
            trim(col("sector_code")).alias("sector_code"),
            trim(col("measure_code")).alias("measure_code"),
            col("_metadata")
        )
        .dropDuplicates(["series_id"])
    )


@dp.table(
    comment="Silver layer: Clean BLS measure lookup"
)
@dp.expect_or_drop("valid_measure_code", "measure_code IS NOT NULL")
def silver_bls_measure():

    return (
        spark.readStream.table("bronze_bls_measure")
        .select(
            trim(col("measure_code")).alias("measure_code"),
            trim(col("measure_text")).alias("measure_text"),
            col("_metadata")
        )
        .dropDuplicates(["measure_code"])
    )


@dp.table(
    comment="Silver layer: Clean BLS sector lookup"
)
@dp.expect_or_drop("valid_sector_code", "sector_code IS NOT NULL")
def silver_bls_sector():

    return (
        spark.readStream.table("bronze_bls_sector")
        .select(
            trim(col("sector_code")).alias("sector_code"),
            trim(col("sector_name")).alias("sector_name"),
            col("_metadata")
        )
        .dropDuplicates(["sector_code"])
    )