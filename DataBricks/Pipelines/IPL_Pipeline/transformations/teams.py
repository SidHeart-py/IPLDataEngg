from pyspark import pipelines as dlt
from pyspark.sql.functions import (
    col,
    to_date,
    current_timestamp,
    lit,
    expr
)
from pyspark.sql.types import IntegerType


# -----------------------
# Bronze: Streaming VIEW
# -----------------------
@dlt.view
def bronze_teams():
    path = r'abfss://bronze-external@ipldatastorage.dfs.core.windows.net/Team/'

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/mnt/schema/team")
            .load(path)
            .withColumn("insert_ts", current_timestamp())
    )


# -----------------------
# Silver: Streaming TABLE
# -----------------------
@dlt.table(
    name="silver_teams",
    comment="Cleaned teams data from bronze",
    table_properties={"quality": "silver"}
)
def silver_teams():

    df = dlt.read_stream("bronze_teams")

    return (
        df
        .drop("Team_SK")
        .withColumn("Team_Id", col("Team_Id").cast(IntegerType()))
        .withColumn("insert_ts", current_timestamp())
    )


# -----------------
# Gold: SCD2 Table
# -----------------
dlt.create_streaming_table("gold_teams")

dlt.create_auto_cdc_flow(
        target="gold_teams",
        source="silver_teams",
        keys=["Team_Id"],
        except_column_list=["insert_ts"],
        stored_as_scd_type=2,
        sequence_by="insert_ts"
    )