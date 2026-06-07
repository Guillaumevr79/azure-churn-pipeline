# Databricks notebook source
from pyspark.sql.functions import col, when, count, avg, sum as spark_sum, datediff, to_date, lit

# COMMAND ----------

storage_account = "adlschurnpipeline"

SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/telco/customers"
GOLD_PATH = f"abfss://gold@{storage_account}.dfs.core.windows.net/telco/features"

# COMMAND ----------

# Lecture Silver
df = spark.read.format("delta").load(SILVER_PATH)
print(f"Lignes Silver : {df.count()}")
df.printSchema()

# COMMAND ----------

# Feature engineering Gold
df_gold = df \
    .withColumn("has_phone_service", when(col("phone_service") == "Yes", 1).otherwise(0)) \
    .withColumn("has_internet", when(col("internet_service") != "No", 1).otherwise(0)) \
    .withColumn("is_month_to_month", when(col("contract") == "Month-to-month", 1).otherwise(0)) \
    .withColumn("is_long_term", when(col("contract") == "Two year", 1).otherwise(0)) \
    .withColumn("charges_per_month_tenure", 
        when(col("tenure") > 0, col("total_charges") / col("tenure"))
        .otherwise(col("monthly_charges"))
    ) \
    .withColumn("high_monthly_charges", when(col("monthly_charges") > 65, 1).otherwise(0)) \
    .withColumn("long_tenure", when(col("tenure") > 24, 1).otherwise(0)) \
    .select(
        "customer_id",
        "senior_citizen",
        "tenure",
        "monthly_charges",
        "total_charges",
        "contract",
        "internet_service",
        "has_phone_service",
        "has_internet",
        "is_month_to_month",
        "is_long_term",
        "charges_per_month_tenure",
        "high_monthly_charges",
        "long_tenure",
        "churn"
    )

print(f"Lignes Gold : {df_gold.count()}")
df_gold.show(5)

# COMMAND ----------

# Écriture Gold (Delta)
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_PATH)

print(f"✓ Gold écrit : {GOLD_PATH}")