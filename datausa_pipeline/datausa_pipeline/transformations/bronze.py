from pyspark import pipelines as dp
from pyspark.sql.functions import col

DATAUSA_PATH = "/Volumes/rearc/datausa/raw/"


@dp.table(
    comment="Bronze layer: Raw DataUSA population snapshots ingested using Auto Loader",
    table_properties={
        "delta.columnMapping.mode": "name"
    }
)
def bronze_population():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(DATAUSA_PATH)
        .select(
            "*",
            col("_metadata").alias("_metadata")
        )
    )