from pyspark import pipelines as dp
from pyspark.sql.functions import avg, stddev, col


@dp.materialized_view(
    comment="Gold: Mean and standard deviation of annual US population from 2013 through 2018"
)
def gold_population_stats():
    return (
        spark.read.table("silver_population")
        .filter(col("year").between(2013, 2018))
        .agg(
            avg("population").alias("mean_population"),
            stddev("population").alias("stddev_population")
        )
    )