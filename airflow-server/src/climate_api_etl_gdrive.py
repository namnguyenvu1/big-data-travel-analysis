import pandas as pd
import gcsfs
import datetime
import pytz
import os
from dotenv import load_dotenv
import subprocess

def fetch_disease_death_data():
    load_dotenv()
    
    # Get the current datetime in UTC and convert to the 'America/Edmonton' timezone
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    edmonton_tz = pytz.timezone('America/Edmonton') 
    edmonton_now = utc_now.astimezone(edmonton_tz) 
    dt = edmonton_now.strftime("%Y-%m-%d %H:%M")

    # Specify your GCS bucket and file path
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    FILE_PATH = f"disease_death_data.csv"

    # Google Drive File ID for the dataset
    file_id = "1FKaZWXoVQ8pIIPFSrRWR8vofTvU20LP6"

    # Path to save the dataset locally
    local_file_path = "/content/IHME-GBD_2021_DATA-60e1e0e5-1.csv"

    # Download the dataset using gdown
    print("===> Downloading dataset from Google Drive")
    subprocess.run(["gdown", "--id", file_id, "--output", local_file_path], check=True)

    # Load the dataset into a Pandas DataFrame
    print("===> Loading dataset into DataFrame")
    disease_death = pd.read_csv(local_file_path)

    # Display a preview of the dataset
    print("===> Data preview")
    print(disease_death.head())

    # Save the DataFrame to Google Cloud Storage
    print("===> Writing to GCS")
    gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
    
    with gcsfs.GCSFileSystem().open(gcs_path, "w") as f:
        disease_death.to_csv(f, index=False)

    print(f"Data successfully written to {gcs_path}")

# Call the function
fetch_disease_death_data()