# Databricks notebook source
# MAGIC %md
# MAGIC **Script Generator**

# COMMAND ----------

import json, random, time, os
from datetime import datetime, UTC

# Use a Databricks volume or DBFS path — adjust to your workspace setup
output_path = "/Volumes/workspace/default/demo_series1/streaming_events/"
os.makedirs(output_path, exist_ok=True)

event_types = ["page_view", "add_to_cart", "purchase"]
products = ["laptop", "phone", "headphones", "monitor", "keyboard"]

def generate_batch(n=20):
    return [
        {
            "user_id": f"user_{random.randint(1, 200)}",
            "event_type": random.choice(event_types),
            "product": random.choice(products),
            "price": round(random.uniform(10, 1500), 2),
            "event_time": datetime.now(UTC).isoformat()
        }
        for _ in range(n)
    ]

for i in range(36):
    batch = generate_batch(50)
    filename = f"{output_path}events_{int(time.time())}.json"
    with open(filename, "w") as f:
        json.dump(batch, f)
    print(f"Wrote {filename}")
    time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Stream with Autoloader**

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, DoubleType

schema = (
    StructType()
    .add("user_id", StringType())
    .add("event_type", StringType())
    .add("product", StringType())
    .add("price", DoubleType())
    .add("event_time", StringType())
)

stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)
    .load("/Volumes/workspace/default/demo_series1/streaming_events/")
)

query = (
    stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/demo_series1/checkpoints/bronze_events/")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("bronze_streaming_events")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Watch IT Land**

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM bronze_streaming_events").display()

# COMMAND ----------

spark.sql("SELECT * FROM bronze_streaming_events ORDER BY event_time DESC LIMIT 10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Read from Bronze as a stream**

# COMMAND ----------

bronze_stream = spark.readStream.table("bronze_streaming_events")

# COMMAND ----------

# MAGIC %md
# MAGIC **Clean and transform**

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, upper, trim

silver_stream = (
    bronze_stream
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("user_id", trim(col("user_id")))
    .withColumn("event_type", trim(upper(col("event_type"))))
    .filter(col("price").isNotNull() & (col("price") > 0))
    .filter(col("event_time").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 — Silver Delta table**

# COMMAND ----------

silver_query = (
    silver_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/demo_series1/checkpoints/silver_events/")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("silver_events")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify**

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM silver_events").display()
spark.sql("SELECT * FROM silver_events ORDER BY event_time DESC LIMIT 10").display()