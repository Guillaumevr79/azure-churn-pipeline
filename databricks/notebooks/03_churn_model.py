# Databricks notebook source
# Pas de %pip install — on utilise les packages pré-installés de DBR 13.3

# COMMAND ----------

# DBTITLE 1,Cell 2
import sys
import typing_extensions

if not hasattr(typing_extensions, "Sentinel"):
    class Sentinel:
        def __init__(self, name, module_name=None):
            self._name = name
            self._module_name = module_name

        def __repr__(self):
            return self._name

    typing_extensions.Sentinel = Sentinel

if not hasattr(typing_extensions, "NoExtraItems"):
    class _NoExtraItemsType:
        def __repr__(self):
            return "NoExtraItems"

    typing_extensions.NoExtraItems = _NoExtraItemsType()

for name in list(sys.modules):
    if name == "mlflow" or name.startswith("mlflow."):
        del sys.modules[name]

import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np

# COMMAND ----------

storage_account = "adlschurnpipeline"
GOLD_PATH = f"abfss://gold@{storage_account}.dfs.core.windows.net/telco/features"

# COMMAND ----------

df = spark.read.format("delta").load(GOLD_PATH)
pdf = df.toPandas()
print(f"Shape : {pdf.shape}")

# COMMAND ----------

le = LabelEncoder()
pdf["contract_enc"] = le.fit_transform(pdf["contract"])
pdf["internet_service_enc"] = le.fit_transform(pdf["internet_service"])

FEATURES = [
    "senior_citizen", "tenure", "monthly_charges", "total_charges",
    "has_phone_service", "has_internet", "is_month_to_month", "is_long_term",
    "charges_per_month_tenure", "high_monthly_charges", "long_tenure",
    "contract_enc", "internet_service_enc"
]
TARGET = "churn"

X = pdf[FEATURES].fillna(0)
y = pdf[TARGET]

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train : {X_train.shape}, Test : {X_test.shape}")

# COMMAND ----------

mlflow.set_experiment("/churn-pipeline/churn-experiment")

with mlflow.start_run(run_name="gradient-boosting-v1"):
    model = GradientBoostingClassifier(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=4,
        random_state=42
    )
    model.fit(X_train, y_train)

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)
    auc = roc_auc_score(y_test, y_pred_proba)

    mlflow.log_params({"n_estimators": 100, "learning_rate": 0.1, "max_depth": 4})
    mlflow.log_metrics({"auc_roc": auc})
    mlflow.sklearn.log_model(model, "churn-model")

    print(f"AUC-ROC : {auc:.4f}")
    print(classification_report(y_test, y_pred, target_names=["No Churn", "Churn"]))

print("✓ Modèle entraîné et loggé dans MLflow")