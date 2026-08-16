from pyspark import pipelines as dp
from pyspark.sql.functions import col

BLS_PATH = "/Volumes/rearc/bls/raw/"


def clean_column_names(df):
    return df.toDF(*[c.strip() for c in df.columns])


@dp.table(
    comment="Bronze layer: Raw BLS productivity observations"
)
def bronze_bls_data():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "pr.data.1.AllData*")
        .load(BLS_PATH)
        .select(
            "*",
            col("_metadata").alias("_metadata")
        )
    )

    return clean_column_names(df)


@dp.table(
    comment="Bronze layer: Raw BLS series metadata"
)
def bronze_bls_series():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "pr.series*")
        .load(BLS_PATH)
        .select(
            "*",
            col("_metadata").alias("_metadata")
        )
    )

    return clean_column_names(df)


@dp.table(
    comment="Bronze layer: Raw BLS measure lookup"
)
def bronze_bls_measure():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "pr.measure*")
        .load(BLS_PATH)
        .select(
            "*",
            col("_metadata").alias("_metadata")
        )
    )

    return clean_column_names(df)


@dp.table(
    comment="Bronze layer: Raw BLS sector lookup"
)
def bronze_bls_sector():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "pr.sector*")
        .load(BLS_PATH)
        .select(
            "*",
            col("_metadata").alias("_metadata")
        )
    )

    return clean_column_names(df)