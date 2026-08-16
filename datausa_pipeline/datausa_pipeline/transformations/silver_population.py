from pyspark import pipelines as dp
from pyspark.sql.functions import explode, col


@dp.table(
    comment="Silver layer: Clean and validated annual US population data"
)
@dp.expect_or_drop(
    "valid_nation_id",
    "nation_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_year",
    "year IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_population",
    "population IS NOT NULL"
)
def silver_population():

    return (
        spark.readStream.table("bronze_population")
        .select(
            explode("data").alias("record"),
            col("_metadata")
        )
        .select(
            col("record.`Nation ID`").alias("nation_id"),
            col("record.Nation").alias("nation"),
            col("record.Year").cast("int").alias("year"),
            col("record.Population").cast("long").alias("population"),
            col("_metadata")
        )
        .dropDuplicates([
            "nation_id",
            "year"
        ])
    )