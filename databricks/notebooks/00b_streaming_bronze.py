# Databricks notebook source
# Streaming consumer : Event Hubs → Bronze Delta (Spark Structured Streaming)
# Protocole Kafka via endpoint Event Hubs Standard

# COMMAND ----------

storage_account = "adlschurnpipeline"
eventhub_namespace = "evhns-churn-pipeline"
eventhub_name = "churn-events"

# Récupération des secrets
eh_connection_string = dbutils.secrets.get(scope="churn-scope", key="eventhub-connection-string")

# Extraction de la SharedAccessKey depuis la connection string
# Format : Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=XXX
import re
sas_key = re.search(r"SharedAccessKey=([^;]+)", eh_connection_string).group(1)
sas_key_name = re.search(r"SharedAccessKeyName=([^;]+)", eh_connection_string).group(1)

# Config Kafka pour Event Hubs
bootstrap_servers = f"{eventhub_namespace}.servicebus.windows.net:9093"
jaas_config = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="$ConnectionString" '
    f'password="{eh_connection_string}";'
)

# COMMAND ----------

# Chemin Bronze Delta pour le streaming
BRONZE_STREAMING_PATH = f"abfss://bronze@{storage_account}.dfs.core.windows.net/telco/streaming"
CHECKPOINT_PATH = f"abfss://bronze@{storage_account}.dfs.core.windows.net/_checkpoints/streaming"

# COMMAND ----------

# Lecture Structured Streaming depuis Event Hubs (protocole Kafka)
df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("subscribe", eventhub_name)
    .option("startingOffsets", "earliest")
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "30000")
    .load()
)

# COMMAND ----------

# Parsing des événements JSON
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

schema = StructType([
    StructField("customer_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("internet_service", StringType()),
    StructField("contract", StringType()),
    StructField("payment_method", StringType()),
    StructField("monthly_charges", DoubleType()),
    StructField("tenure_months", IntegerType()),
    StructField("support_calls", IntegerType()),
    StructField("late_payments", IntegerType()),
])

df_parsed = (
    df_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

# Écriture Bronze Delta en streaming (trigger once = batch unique pour le portfolio)
query = (
    df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start(BRONZE_STREAMING_PATH)
)

query.awaitTermination()
print(f"✓ Streaming terminé — données écrites dans {BRONZE_STREAMING_PATH}")