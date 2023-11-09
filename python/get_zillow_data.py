import os
import sys
import argparse
import requests
import csv
import json
import re
import pandas as pd
from datetime import datetime
import time
from google.cloud import storage
from dotenv import load_dotenv
import logging

logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)  # Set logger level

# Create a file handler and set its level to DEBUG
file_handler = logging.FileHandler("get_zillow_data.log")
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


BUCKET_NAME = "dreamhome1029"
# The date in filename doesn't mean anything, will rename it later.
DESTINATION_BLOB_NAME = "forsale_231028.csv"

# Load environment variables from .env file
load_dotenv()

if os.environ.get("X_RAPIDAPI_KEY") is None:
    raise Exception("Envionment variable X_RAPIDAPI_KEY is not set")

STATE = "ca"
if os.environ.get("STATE") is not None:
    STATE = os.environ.get("STATE")

API_BASEURL = "https://zillow-com1.p.rapidapi.com"
ZILLOW_BASEURL = "https://zillow.com"

HEADERS = {
    "X-RapidAPI-Key": os.environ.get("X_RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
}


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


def write_to_csv(json_content, csv_filename):
    # Opening the file with write permission
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        if isinstance(json_content, str):
            json_content = json.loads(json_content)
        if json_content:  # Check if json_content is not empty
            header = json_content[0].keys()
            writer = csv.DictWriter(file, fieldnames=header)

            # Writing the header
            writer.writeheader()

            # Writing the rows
            writer.writerows(json_content)
            logger.info(f"{csv_filename} is created")
        else:
            logger.error("No data to write!")


def getZipCode(address):
    # Search for a sequence of 5 digits in the address string.
    match = re.search(r"\b\d{5}$", address)
    # Return the matched string if found, otherwise return None.
    return match.group(0) if match else ""


def getDetailByZpid(zpid):
    time.sleep(1)
    logger.info(f"getDetailByZpid {zpid}")
    url = f"{API_BASEURL}/property"
    querystring = {"zpid": zpid}
    response = requests.get(url, headers=HEADERS, params=querystring)
    responseJson = response.json()
    # print(responseJson)
    addressInfo = responseJson["address"]
    address = addressInfo["streetAddress"]
    city = addressInfo["city"]
    zipcode = addressInfo["zipcode"]
    bedrooms = responseJson["bedrooms"]
    datePosted = responseJson["datePosted"]
    dateSold = responseJson["dateSold"]
    livingArea = responseJson["livingArea"]
    lotSize = responseJson["resoFacts"]["lotSize"]
    stories = responseJson["resoFacts"]["stories"]
    rentZestimate = responseJson["rentZestimate"]
    propertyTaxRate = responseJson["propertyTaxRate"]
    yearBuilt = responseJson["yearBuilt"]
    zestimate = responseJson["zestimate"]
    homeStatus = responseJson["homeStatus"]
    price = responseJson["price"]
    pricePerFt = -1
    if livingArea != 0:
        pricePerFt = round(price / livingArea, 2)
    listingPrice = -1
    historyEntries = responseJson["priceHistory"]
    pathLink = responseJson["url"]
    link = f"{ZILLOW_BASEURL}/{pathLink}"

    schools = responseJson["schools"]
    schoolsE = ""
    schoolsM = ""
    schoolsH = ""
    for school in schools:
        if school["level"] == "Elementary":
            schoolsE = f"""{school["rating"]},{school["name"]}"""
            continue
        if school["level"] == "Middle":
            schoolsM = f"""{school["rating"]},{school["name"]}"""
            continue
        if school["level"] == "High":
            schoolsH = f"""{school["rating"]},{school["name"]}"""

    for entry in historyEntries:
        if entry["event"] == "Listed for sale":
            listingPrice = entry["price"]
            break
    return {
        "zpid": zpid,
        "address": address,
        "city": city,
        "zipcode": zipcode,
        "homeStatus": homeStatus,
        "price": price,
        "pricePerFt": pricePerFt,
        "listingPrice": listingPrice,
        "livingArea": livingArea,
        "lotSize": lotSize,
        "zestimate": zestimate,
        "bedrooms": bedrooms,
        "datePosted": datePosted,
        "dateSold": dateSold,
        "stories": stories,
        "rentZestimate": rentZestimate,
        "propertyTaxRate": propertyTaxRate,
        "yearBuilt": yearBuilt,
        "schoolsE": schoolsE,
        "schoolsM": schoolsM,
        "schoolsH": schoolsH,
        "link": link,
    }


def getBasicData(cities, statusType, days):
    querystring = {
        "home_type": "Houses",
        "bedsMin": 3,
        "sort": "Price_High_Low",
        "status_type": statusType,
    }
    if statusType == "RecentlySold":
        querystring["soldInLast"] = days

    allProps = []
    url = f"{API_BASEURL}/propertyExtendedSearch"
    cities_list = cities.split(",")
    for city in cities_list:
        queryWithPage = querystring.copy()
        queryWithPage["location"] = f"{city}, {STATE}"
        pageIdx = 1
        totalPages = 1000
        while pageIdx <= totalPages:
            logger.debug(f"pageIdx: {pageIdx}")
            queryWithPage["page"] = pageIdx
            time.sleep(1)
            response = requests.get(url, headers=HEADERS, params=queryWithPage)
            responseJson = response.json()
            if "props" not in responseJson:
                raise Exception(
                    f"Failed to GET {url}, please check your API token in .env. "
                    + f"{responseJson}"
                )
            props = responseJson["props"]
            keysToExtract = [
                "price",
                "zestimate",
                "livingArea",
                "lotAreaValue",
                "zpid",
                "rentZestimate",
                "bedrooms",
                "propertyType",
                "address",
            ]
            # Creating a new dictionary with only the selected keys
            keyProps = []
            for prop in props:
                keyProp = {key: prop[key] for key in keysToExtract}
                zipcode = getZipCode(prop["address"])
                keyProp["zipcode"] = zipcode
                keyProp["city"] = city
                dateSoldTimeStamp = prop["dateSold"] / 1000
                dateSold = datetime.fromtimestamp(dateSoldTimeStamp).strftime(
                    "%Y-%m-%d"
                )
                keyProp["dateSold"] = dateSold
                if prop["price"] is None or prop["livingArea"] is None:
                    pricePerFt = -1
                else:
                    pricePerFt = round(prop["price"] / prop["livingArea"], 2)
                keyProp["pricePerFt"] = pricePerFt
                keyProps.append(keyProp)
            allProps.extend(keyProps)
            totalPages = responseJson["totalPages"]
            pageIdx += 1

    formatted_date = datetime.now().strftime("%y%m%d")
    outputFile = f"{statusType.lower()}_{formatted_date}_basic.csv"
    write_to_csv(allProps, outputFile)
    return outputFile


def getForSaleData(cities):
    querystring = {
        "home_type": "Houses",
        "bedsMin": 3,
        "sort": "Price_High_Low",
        "status_type": "ForSale",
    }
    allProps = []
    formatted_date = datetime.now().strftime("%y%m%d")
    url = f"{API_BASEURL}/propertyExtendedSearch"
    cities_list = cities.split(",")
    zpids = []
    for city in cities_list:
        queryWithPage = querystring.copy()
        queryWithPage["location"] = f"{city}, {STATE}"
        pageIdx = 1
        totalPages = 1000
        while pageIdx <= totalPages:
            queryWithPage["page"] = pageIdx
            response = requests.get(url, headers=HEADERS, params=queryWithPage)
            responseJson = response.json()
            if "props" not in responseJson:
                raise Exception(
                    f"Failed to GET {url}, please check your API token in .env. "
                    + f"{responseJson}"
                )
            # print(responseJson)
            props = responseJson["props"]
            for prop in props:
                zpids.append(prop["zpid"])
            totalPages = responseJson["totalPages"]
            pageIdx += 1
    inventoryTodayFile = f"forsale_{formatted_date}.csv"
    oldZpids = []
    if os.path.isfile(inventoryTodayFile):
        oldDf = pd.read_csv(inventoryTodayFile)
        oldDf["zpid"] = oldDf["zpid"].astype(str)
        oldZpids = oldDf["zpid"].tolist()
        logger.info(oldZpids)
    for zpid in zpids:
        if zpid in oldZpids:
            row = oldDf[oldDf["zpid"] == zpid]
            logger.info(f"skip existing {zpid}")
            propInfo = {
                "zpid": zpid,
                "address": row["address"].values[0],
                "city": row["city"].values[0],
                "zipcode": row["zipcode"].values[0],
                "homeStatus": row["homeStatus"].values[0],
                "price": row["price"].values[0],
                "pricePerFt": row["pricePerFt"].values[0],
                "listingPrice": row["listingPrice"].values[0],
                "livingArea": row["livingArea"].values[0],
                "lotSize": row["lotSize"].values[0],
                "zestimate": row["zestimate"].values[0],
                "bedrooms": row["bedrooms"].values[0],
                "datePosted": row["datePosted"].values[0],
                "dateSold": row["dateSold"].values[0],
                "stories": row["stories"].values[0],
                "rentZestimate": row["rentZestimate"].values[0],
                "propertyTaxRate": row["propertyTaxRate"].values[0],
                "yearBuilt": row["yearBuilt"].values[0],
                "schoolsE": row["schoolsE"].values[0],
                "schoolsM": row["schoolsM"].values[0],
                "schoolsH": row["schoolsH"].values[0],
                "link": row["link"].values[0],
            }
        else:
            propInfo = getDetailByZpid(zpid)
        allProps.append(propInfo)

    # print(allProps)
    write_to_csv(allProps, inventoryTodayFile)
    return inventoryTodayFile


def getRecentSoldData(cities, recentDays):
    querystring = {
        "home_type": "Houses",
        "bedsMin": 3,
        "sort": "Price_High_Low",
        "soldInLast": str(recentDays),
        "status_type": "RecentlySold",
    }
    url = f"{API_BASEURL}/propertyExtendedSearch"
    allProps = []
    cities_list = cities.split(",")
    for city in cities_list:
        queryWithPage = querystring.copy()
        queryWithPage["location"] = f"{city}, {STATE}"
        pageIdx = 1
        totalPages = 1000
        while pageIdx <= totalPages:
            queryWithPage["page"] = pageIdx
            response = requests.get(url, headers=HEADERS, params=queryWithPage)
            # print(response.status_code)
            responseJson = response.json()
            if "props" not in responseJson:
                raise Exception(
                    f"Failed to GET {url}, please check your API token in .env"
                )
            # print(responseJson)
            props = responseJson["props"]
            # print(props[0])
            for prop in props:
                zpid = prop["zpid"]
                propInfo = getDetailByZpid(zpid)
                allProps.append(propInfo)
            totalPages = responseJson["totalPages"]
            pageIdx += 1
    formatted_date = datetime.now().strftime("%y%m%d")
    outputFile = f"sold_{formatted_date}.csv"
    write_to_csv(allProps, outputFile)
    return outputFile


def main(args):
    cities = args.cities
    statusType = args.status
    recentDays = args.days
    mode = args.mode
    if statusType not in ["ForSale", "RecentlySold"]:
        logger.error(f"Incorrect status: {statusType}")
        sys.exit(-1)
    if mode not in ["basic", "advanced"]:
        logger.error(f"Incorrect mode: {mode}")
        sys.exit(-1)

    if statusType == "ForSale":
        if mode == "basic":
            if args.upload:
                logger.warning("Upload option is not available for this mode")
            else:
                getBasicData(cities, statusType, recentDays)
        else:
            outputFile = getForSaleData(cities)
            if args.upload:
                uploadBlob(outputFile)
    else:
        if args.upload:
            logger.warning("Upload option is not available for this mode")
            return
        if mode == "basic":
            getBasicData(cities, statusType, recentDays)
        else:
            getRecentSoldData(cities, recentDays)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get city house data from Zillow")
    parser.add_argument("--cities", required=True, help="City Name")
    parser.add_argument("--status", default="ForSale", help="[ForSale|RecentlySold]")
    parser.add_argument("--mode", default="basic", help="[basic|advanced]")
    parser.add_argument(
        "--days", default=7, help="recent sold days, only used for RecentlySold option"
    )
    uploadHelpMsg = "Enable Upload to GCP mode, only work for ForSale advanced mode"
    parser.add_argument("--upload", action="store_true", help=uploadHelpMsg)
    args = parser.parse_args()
    main(args)
