import argparse
import requests
import csv
import json
from datetime import datetime
import time
import os


from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

if os.environ.get("X_RAPIDAPI_KEY") is None:
    raise Exception("Envionment variable X_RAPIDAPI_KEY is not set")

STATE = "ca"
if os.environ.get("STATE") is not None:
    STATE = os.environ.get("STATE")

API_BASEURL = "https://zillow-com1.p.rapidapi.com"

HEADERS = {
    "X-RapidAPI-Key": os.environ.get("X_RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
}

MARKED_CONFIG_FILE = "marked.csv"


def getPicUrls(zpid):
    time.sleep(1)
    url = f"{API_BASEURL}/images"
    querystring = {"zpid": zpid}
    print(f"querystring: {querystring}")
    response = requests.get(url, headers=HEADERS, params=querystring)
    responseJson = response.json()
    urls = responseJson["images"]
    return urls


def downloadFile(url, save_path):
    """
    Download a file from a given URL and save it to a specified path.

    :param url: URL of the file to be downloaded
    :param save_path: Path where the downloaded file will be saved
    """
    # Send a GET request to the URL
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

    # Write the content of the response to the specified save_path
    with open(save_path, "wb") as file:
        for chunk in response.iter_content(
            chunk_size=8192
        ):  # Download in chunks of 8KB
            file.write(chunk)
    print(f"File downloaded successfully to {save_path}.")


def downloadPics(zpid, urls):
    dirPath = f"pics/{zpid}"
    if not os.path.exists(dirPath):
        os.makedirs(dirPath)
    for url in urls:
        filename = os.path.basename(url)
        filePath = f"{dirPath}/{filename}"
        downloadFile(url, filePath)


def getMarkedZpids():
    with open(MARKED_CONFIG_FILE, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        zpids = [int(row["zpid"]) for row in reader]
    return zpids


def appendNewZpidToMarkedConfig(zpid):
    nowDate = datetime.now().strftime("%y%m%d")
    with open(MARKED_CONFIG_FILE, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([zpid, nowDate])


def getDetailJson(zpid):
    time.sleep(1)
    url = f"{API_BASEURL}/property"
    querystring = {"zpid": zpid}
    response = requests.get(url, headers=HEADERS, params=querystring)
    responseJson = response.json()
    dirPath = f"pics/{zpid}"
    if not os.path.exists(dirPath):
        os.makedirs(dirPath)
    detailJsonPath = f"{dirPath}/detail.json"
    with open(detailJsonPath, "w") as fp:
        fp.write(json.dumps(responseJson, indent=4, sort_keys=True))


def main(args):
    zpids = args.zpids
    zpidList = zpids.split(",")
    markedZpids = getMarkedZpids()
    for zpid in zpidList:
        if zpid in markedZpids:
            continue
        urls = getPicUrls(zpid)
        downloadPics(zpid, urls)
        getDetailJson(zpid)
        appendNewZpidToMarkedConfig(zpid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mark house for future reference")
    parser.add_argument("--zpids", required=True, help="zpids")
    args = parser.parse_args()
    main(args)
