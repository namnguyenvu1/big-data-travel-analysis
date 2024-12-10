import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import gcsfs
import datetime
import pytz

def fetch_unesco_whs_data():
    load_dotenv()
    
    # Get the current datetime in UTC and convert to the 'America/Edmonton' timezone
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    edmonton_tz = pytz.timezone('America/Edmonton') 
    edmonton_now = utc_now.astimezone(edmonton_tz) 
    dt = edmonton_now.strftime("%Y-%m-%d %H:%M")

    # Specify your GCS bucket and file path
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    FILE_PATH = f"infrastructure.csv"

    # URL for the UNESCO WHS Wikipedia page
    url = "https://worldpopulationreview.com/country-rankings/infrastructure-by-country"

    # Fetch the webpage
    print("===> Making request to fetch UNESCO WHS data")
    response = requests.get(url)

    if response.status_code == 200:
        print("===> Webpage successfully fetched")
        
        # Parse the webpage content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the first table on the page
        table = soup.find('table')  # Assumes the target table is the first one

        # Convert the table to a Pandas DataFrame
        print("===> Parsing the table")
        df = pd.read_html(str(table))[0]

        print("===> Data preview")
        print(df.head())

        # Save the DataFrame to Google Cloud Storage
        print("===> Writing to GCS")
        gcs_path = f"gs://{BUCKET_NAME}/{FILE_PATH}"
        
        with gcsfs.GCSFileSystem().open(gcs_path, "w") as f:
            df.to_csv(f, index=False)

        print(f"Data successfully written to {gcs_path}")
    else:
        print(f"Failed to fetch webpage. HTTP Status Code: {response.status_code}")

# Call the function
fetch_unesco_whs_data()
