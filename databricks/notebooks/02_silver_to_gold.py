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

# Contrôles qualité Gold
print("Contrôles qualité...")

# 1. Pas de nulls sur les colonnes critiques
critical_cols = ["customer_id", "monthly_charges", "churn"]
for col_name in critical_cols:
    null_count = df_gold.filter(col(col_name).isNull()).count()
    assert null_count == 0, f"QUALITÉ KO : {null_count} nulls dans {col_name}"
    print(f"✓ {col_name} — 0 null")

# 2. Unicité customer_id
total = df_gold.count()
distinct = df_gold.select("customer_id").distinct().count()
assert total == distinct, f"QUALITÉ KO : doublons customer_id ({total} lignes, {distinct} distincts)"
print(f"✓ customer_id — {total} lignes uniques")

# 3. Churn uniquement 0 ou 1
invalid_churn = df_gold.filter(~col("churn").isin([0, 1])).count()
assert invalid_churn == 0, f"QUALITÉ KO : {invalid_churn} valeurs invalides dans churn"
print(f"✓ churn — valeurs valides (0/1 uniquement)")

# 4. Volume minimum
assert total >= 5000, f"QUALITÉ KO : volume insuffisant ({total} lignes, minimum attendu 5000)"
print(f"✓ volume — {total} lignes (seuil 5000 ok)")

print("Tous les contrôles qualité passés ✓")

# COMMAND ----------

# Écriture Gold (Delta)
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_PATH)

print(f"✓ Gold écrit : {GOLD_PATH}")