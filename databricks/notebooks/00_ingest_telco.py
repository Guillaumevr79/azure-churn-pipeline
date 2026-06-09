# Databricks notebook source
import subprocess
import sys

# COMMAND ----------

# Installation des dépendances
%pip install kaggle azure-storage-blob python-dotenv -q
dbutils.library.restartPython()

# COMMAND ----------

import os
import zipfile
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# Config
storage_account = "adlschurnpipeline"
storage_key = dbutils.secrets.get(scope="churn-scope", key="storage-key")
container_name = "bronze"
adls_prefix = "telco/raw"
file_name = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
raw_dir = "/tmp/telco"

# COMMAND ----------

# Téléchargement Kaggle
os.makedirs(raw_dir, exist_ok=True)
os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get(scope="churn-scope", key="kaggle-username")
os.environ["KAGGLE_KEY"] = dbutils.secrets.get(scope="churn-scope", key="kaggle-key")

import kaggle
kaggle.api.dataset_download_files(
    "blastchar/telco-customer-churn",
    path=raw_dir,
    quiet=False,
    unzip=True
)
print("✓ Dataset téléchargé")

# COMMAND ----------

# Upload vers ADLS Bronze
client = BlobServiceClient(
    account_url=f"https://{storage_account}.blob.core.windows.net",
    credential=storage_key
)
container = client.get_container_client(container_name)
local_path = os.path.join(raw_dir, file_name)
blob_path = f"{adls_prefix}/{file_name}"

with open(local_path, "rb") as f:
    container.upload_blob(name=blob_path, data=f, overwrite=True)

print(f"✓ {file_name} uploadé vers {container_name}/{blob_path}")