# Databricks notebook source
from pyspark.sql.functions import col, when, trim, lower

# COMMAND ----------

# OAuth configuré au niveau cluster via spark_conf (secrets/churn-scope)
storage_account = "adlschurnpipeline"

# COMMAND ----------

# Chemins
BRONZE_PATH = f"abfss://bronze@{storage_account}.dfs.core.windows.net/telco/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv"
SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/telco/customers"

# COMMAND ----------

# Lecture Bronze
df = spark.read.csv(BRONZE_PATH, header=True, inferSchema=True)
print(f"Lignes lues : {df.count()}")
df.printSchema()

# COMMAND ----------

# Nettoyage Silver
df_silver = df \
    .withColumn("TotalCharges",
        when(trim(col("TotalCharges")) == "", None)
        .otherwise(col("TotalCharges").cast("double"))
    ) \
    .withColumn("Churn", when(lower(col("Churn")) == "yes", 1).otherwise(0)) \
    .withColumn("SeniorCitizen", col("SeniorCitizen").cast("integer")) \
    .withColumn("tenure", col("tenure").cast("integer")) \
    .withColumn("MonthlyCharges", col("MonthlyCharges").cast("double")) \
    .dropDuplicates(["customerID"]) \
    .dropna(subset=["customerID", "MonthlyCharges"])

# Renommage snake_case
df_silver = df_silver \
    .withColumnRenamed("customerID", "customer_id") \
    .withColumnRenamed("SeniorCitizen", "senior_citizen") \
    .withColumnRenamed("PhoneService", "phone_service") \
    .withColumnRenamed("InternetService", "internet_service") \
    .withColumnRenamed("Contract", "contract") \
    .withColumnRenamed("MonthlyCharges", "monthly_charges") \
    .withColumnRenamed("TotalCharges", "total_charges") \
    .withColumnRenamed("Churn", "churn")

print(f"Lignes après nettoyage : {df_silver.count()}")
df_silver.show(5)

# COMMAND ----------

# Écriture Silver (Delta)
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

print(f"✓ Silver écrit : {SILVER_PATH}")