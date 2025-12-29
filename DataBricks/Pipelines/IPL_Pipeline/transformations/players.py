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
def bronze_players():
    path = "abfss://bronze-external@ipldatastorage.dfs.core.windows.net/Players/"

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/mnt/schema/players")
            .load(path)
            .withColumn("insert_ts", current_timestamp())
    )


# -----------------------
# Silver: Streaming TABLE
# -----------------------
@dlt.table(
    name="silver_players",
    comment="Cleaned players data from bronze",
    table_properties={"quality": "silver"}
)
def silver_players():

    df = dlt.read_stream("bronze_players")

    return (
        df
        .drop("insert_ts", "file_name", "PLAYER_SK")
        .withColumn("Player_Id", col("Player_Id").cast(IntegerType()))
        .withColumn("DOB", to_date(col("DOB"), 'M/d/yyyy'))
        .withColumn("insert_ts", current_timestamp())
    )


# -----------------
# Gold: SCD2 Table
# -----------------
dlt.create_streaming_table("gold_players")

dlt.create_auto_cdc_flow(
        target="gold_players",
        source="silver_players",
        keys=["Player_Id"],
        except_column_list=["insert_ts"],
        stored_as_scd_type=2,
        sequence_by="insert_ts"
    )