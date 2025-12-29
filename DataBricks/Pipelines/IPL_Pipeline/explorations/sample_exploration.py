# Databricks notebook source
df = spark.read.table('ipl.enriched.silver_players')
df.display()

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.drop("insert_ts", 'file_name', 'PLAYER_SK')

# COMMAND ----------

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, StringType

df = (
    df
    # Numeric keys
    .withColumn("Player_Id", col("Player_Id").cast(IntegerType()))
    
    # Date column
    .withColumn("DOB", to_date(col("DOB"), "yyyy-MM-dd"))
)

