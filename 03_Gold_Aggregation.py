# Databricks notebook source
# MAGIC %md
# MAGIC **Silver as a stream, with a watermark**

# COMMAND ----------

from pyspark.sql.functions import window, col, sum as _sum, count

silver_stream = spark.readStream.table("silver_events")

gold_stream = (
    silver_stream
    .withWatermark("event_time", "10 minutes")
    .filter(col("event_type") == "PURCHASE")
    .groupBy(window(col("event_time"), "5 minutes"))
    .agg(
        _sum("price").alias("total_revenue"),
        count("*").alias("num_purchases")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **OutputMode changes here**

# COMMAND ----------

gold_query = (
    gold_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/demo_series1/checkpoints/gold_revenue/")
    .outputMode("complete")   # not "append" — aggregates get updated as new data arrives
    .trigger(availableNow=True)
    .table("gold_revenue_by_window")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify**

# COMMAND ----------

spark.sql("SELECT * FROM gold_revenue_by_window ORDER BY window DESC").display()