import os
import urllib.request
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import gzip
import shutil
import time

# Configurazione GCP
BUCKET_NAME = "dezoomcamp_ag15_hw3_2025"
CREDENTIALS_FILE = "C:/Users/Aless/Documents/GIT/data-engineering-zoomcamp-project-repo/03_data_warehouse/de-zoomcamp-ag15-ce2b96ea670a.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

# Parametri
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "C:/Users/Aless/Documents/GIT/data-engineering-zoomcamp-project-repo/03_data_warehouse/"
CHUNK_SIZE = 8 * 1024 * 1024

# Creazione directory se non esiste
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)

# Mappa per correggere i tipi di colonna
dtype_mapping = {
    "PUlocationID": "Int64",
    "DOlocationID": "Int64"
}

def download_and_convert_file(month):
    url = f"{BASE_URL}{month}.csv.gz"
    csv_gz_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv.gz")
    csv_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv")
    parquet_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, csv_gz_path)
        print(f"Downloaded: {csv_gz_path}")

        # Estrarre il file CSV
        with gzip.open(csv_gz_path, 'rb') as f_in, open(csv_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        # Convertire in Parquet con tipi corretti
        df = pd.read_csv(csv_path, dtype=dtype_mapping)
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
        print(f"Converted to Parquet: {parquet_path}")

        # Pulizia file CSV intermedi
        os.remove(csv_gz_path)
        os.remove(csv_path)

        return parquet_path
    except Exception as e:
        print(f"Error processing {month}: {e}")
        return None

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_and_convert_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    print("All files processed and uploaded successfully.")
