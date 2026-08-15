from pyspark import pipelines as dp

@dp.table(

    comment="Bronze layer: Raw DataUSA population data ingested using Auto Loader",
    table_properties={
        "delta.columnMapping.mode": "name"
    }

)
def bronze_population():
    """
    Incrementally ingest population.json from the DataUSA data source directory.
    Auto Loader automatically infers schema from JSON files and handles schema evolution.
    New columns are added automatically when detected.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/rearc/data-quest/step1_folder/datausa/")
        .select(
            "*",
            col("_metadata").alias("_metadata")
        )
    )