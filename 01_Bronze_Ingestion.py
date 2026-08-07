# Databricks notebook source
# MAGIC %md
# MAGIC **Create Raw Data Set for Streaming**

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime, timedelta
import random

# Simulate e-commerce events (similar shape to what you'll stream later)
event_types = ["page_view", "add_to_cart", "purchase"]
products = ["laptop", "phone", "headphones", "monitor", "keyboard"]

data = []
base_time = datetime(2026, 8, 1, 9, 0, 0)
for i in range(2000):
    data.append(Row(
        user_id=f"user_{random.randint(1, 200)}",
        event_type=random.choice(event_types),
        product=random.choice(products),
        price=round(random.uniform(10, 1500), 2),
        event_time=base_time + timedelta(seconds=random.randint(0, 3600*5))
    ))

df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Print Schema**

# COMMAND ----------

df.printSchema()
df.count()
df.groupBy("event_type").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Filter & Select**

# COMMAND ----------

purchases = df.filter(df.event_type == "purchase").select("user_id", "product", "price", "event_time")
purchases.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **GroupBy & Aggregation**

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count, avg

revenue_by_product = (
    df.filter(df.event_type == "purchase")
      .groupBy("product")
      .agg(
          _sum("price").alias("total_revenue"),
          count("*").alias("num_purchases"),
          avg("price").alias("avg_price")
      )
      .orderBy("total_revenue", ascending=False)
)
revenue_by_product.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **With Column**

# COMMAND ----------

from pyspark.sql.functions import when, col

df_flagged = df.withColumn(
    "high_value",
    when(col("price") > 800, True).otherwise(False)
)
df_flagged.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Delta table "Bronze layer**

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("bronze_events")

spark.sql("SELECT event_type, COUNT(*) FROM bronze_events GROUP BY event_type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Left-join need this for Silver-layer enrichment later**

# COMMAND ----------

product_catalog = spark.createDataFrame([
    ("laptop", "Electronics"), ("phone", "Electronics"),
    ("headphones", "Accessories"), ("monitor", "Electronics"),
    ("keyboard", "Accessories")
], ["product", "category"])

enriched = df.join(product_catalog, on="product", how="left")
enriched.groupBy("category").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Top 5 users by total purchase amount**