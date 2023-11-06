import pandas as pd
import re
import os
from datetime import datetime
import argparse
from google.cloud import storage
from dotenv import load_dotenv
import logging

BUCKET_NAME = "dreamhome1029"
# The date in filename doesn't mean anything, will rename it later.
DESTINATION_BLOB_NAME = "overall_summary_forsale.csv"


logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)  # Set logger level

# Create a file handler and set its level to DEBUG
file_handler = logging.FileHandler("get_zillow_summary.log")
file_handler.setLevel(logging.DEBUG)

# Create a console handler and set its level to DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for both handlers
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


load_dotenv()

def uploadBlob(source_file_name):
    """Uploads a file to the bucket."""
    # Set the environment variable for authentication
    # Create a client
    storage_client = storage.Client()
    # Get the bucket
    bucket = storage_client.bucket(BUCKET_NAME)

    # Create a blob and upload the file
    blob = bucket.blob(DESTINATION_BLOB_NAME)
    blob.upload_from_filename(source_file_name)

    logger.info(
        f"File {source_file_name} uploaded to gs://{BUCKET_NAME}/{DESTINATION_BLOB_NAME}."
    )


def get_all_summary(csv_dir):
    files = os.listdir(csv_dir)
    # Filter files that start with 'forsale_'
    forsale_files = [
        file for file in files if file.startswith("forsale_") and file.endswith(".csv")
    ]
    print(forsale_files)
    df = pd.DataFrame()
    for csv_file in forsale_files:
        summary_df = get_summary_df(csv_file)
        df = pd.concat([df, summary_df], ignore_index=True)
    return df


def extract_date_from_filename(filename):
    # Use a regular expression to find a sequence of eight digits
    match = re.search(r"forsale_(\d{2})(\d{2})(\d{2})\.csv$", filename)
    if match:
        # Extract day, month, and year from the match groups
        year, month, day = match.groups()
        # Rearrange and format the date string
        date_str = f"20{year}-{month}-{day}"
        return date_str
    else:
        raise ValueError("No valid date found in filename.")


def get_summary_df(csv_file):
    # Make sure to replace 'your_file.csv' with the path to your actual CSV file
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    collectedDate = extract_date_from_filename(csv_file)

    df["datePosted"] = pd.to_datetime(df["datePosted"])
    df["zipcode"] = df["zipcode"].astype(str)
    df["onMarketDays"] = (datetime.now() - df["datePosted"]).dt.days
    df["collectedDate"] = pd.to_datetime(collectedDate)
    df["price"] = df["price"].replace("[\$,]", "", regex=True).astype(float)

    # Group by 'zipcode' and calculate median and count
    summary_by_zipcode = (
        df.groupby("zipcode")
        .agg(
            median_price=("price", "median"),
            count=("price", "count"),
            onMarketDays=("onMarketDays", "median"),
        )
        .reset_index()
    )

    summary_by_zipcode["collectedDate"] = df["collectedDate"].iloc[0]

    # Calculate the overall median and total count for a new summary row
    overall_median = df["price"].median()
    total_count = df["price"].count()
    onMarketDays = df["onMarketDays"].median()

    # Append the overall summary row to the DataFrame
    overall_summary = pd.DataFrame(
        {
            "zipcode": "All",
            "median_price": overall_median,
            "count": total_count,
            "onMarketDays": onMarketDays,
            "collectedDate": pd.to_datetime(
                collectedDate, format="%Y-%m-%d"
            ),  # Use the same date for the overall summary
        },
        index=[0],
    )

    # Append the overall summary to the summary DataFrame
    summary_by_zipcode = pd.concat(
        [summary_by_zipcode, overall_summary], ignore_index=True
    )
    return summary_by_zipcode


def main(args):
    csv_dir = args.dir
    df = get_all_summary(csv_dir)
    df.to_csv(DESTINATION_BLOB_NAME, index=False)
    print(f"{DESTINATION_BLOB_NAME} is created.")
    uploadBlob(DESTINATION_BLOB_NAME)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get summary from a given forsale csv file"
    )
    parser.add_argument("--dir", required=False, help="forsale csv dir", default=".")
    args = parser.parse_args()
    main(args)
