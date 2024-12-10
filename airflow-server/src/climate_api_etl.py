import pandas as pd
import requests
from dotenv import load_dotenv
import os
import gcsfs
import datetime
import pytz
import os

def fetch_co2_emissions_data():
    load_dotenv()
    # Get the current datetime in UTC and convert to the 'America/Edmonton' timezone
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    edmonton_tz = pytz.timezone('America/Edmonton') 
    edmonton_now = utc_now.astimezone(edmonton_tz) 
    dt = edmonton_now.strftime("%Y-%m-%d %H:%M")

    # Specify your GCS bucket and file path
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    FILE_PATH = f"annual_co2_emissions.csv"

    # URL for the CO2 emissions dataset
    url = "https://ourworldindata.org/grapher/annual-co2-emissions-per-country.csv?v=1&csvType=full&useColumnShortNames=false"
    
    # Add headers to mimic a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # Fetch the data from the URL
    print("===> Making request to fetch CO2 emissions data")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("===> Data successfully fetched")
        # Convert the response content into a Pandas DataFrame
        from io import StringIO
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)

        print("===> Data preview")
        print(df.head())

        print("===> Writing to GCS")
        gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"

        # Write the data to Google Cloud Storage
        with gcsfs.GCSFileSystem().open(gcs_path, "w") as f:
            df.to_csv(f, index=False)

        print(f"Data successfully written to {gcs_path}")
    else:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")

# Call the function
fetch_co2_emissions_data()
