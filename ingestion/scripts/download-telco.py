import os
import kaggle
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = "./data/raw"
CONTAINER_NAME = "bronze"
ADLS_PREFIX = "telco/raw"
FILE_NAME = "WA_Fn-UseC_-Telco-Customer-Churn.csv"

def download_telco():
    os.makedirs(RAW_DIR, exist_ok=True)
    print("Téléchargement du dataset Telco Churn...")
    kaggle.api.dataset_download_files(
        "blastchar/telco-customer-churn",
        path=RAW_DIR,
        quiet=False,
        unzip=True
    )
    print("Téléchargement terminé.")

def upload_to_adls():
    client = BlobServiceClient(
        account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net",
        credential=os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    )
    container = client.get_container_client(CONTAINER_NAME)
    local_path = os.path.join(RAW_DIR, FILE_NAME)
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Fichier manquant : {local_path}")
    blob_path = f"{ADLS_PREFIX}/{FILE_NAME}"
    print(f"Upload {FILE_NAME} → {CONTAINER_NAME}/{blob_path}")
    with open(local_path, "rb") as f:
        container.upload_blob(name=blob_path, data=f, overwrite=True)
    print(f"✓ {FILE_NAME} uploadé.")

if __name__ == "__main__":
    download_telco()
    upload_to_adls()
    print("Ingestion Telco terminée.")