import os
import sys
import argparse
import requests
import csv
import json
import re
from datetime import datetime
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

if os.environ.get("X_RAPIDAPI_KEY") is None:
    raise Exception("Envionment variable X_RAPIDAPI_KEY is not set")


baseUrl = "https://zillow-com1.p.rapidapi.com"


headers = {
    "X-RapidAPI-Key": os.environ.get("X_RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
}


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
            print(f"{csv_filename} is created")
        else:
            print("No data to write!")


def getZipCode(address):
    # Search for a sequence of 5 digits in the address string.
    match = re.search(r"\b\d{5}$", address)
    # Return the matched string if found, otherwise return None.
    return match.group(0) if match else ""


def getDetailByZpid(zpid):
    time.sleep(1)
    url = f"{baseUrl}/property"
    querystring = {"zpid": zpid}
    response = requests.get(url, headers=headers, params=querystring)
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
    pricePerFt = round(price / livingArea, 2)
    listingPrice = -1
    historyEntries = responseJson["priceHistory"]

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
    }


def getForSaleData(cities):
    querystring = {
        "home_type": "Houses",
        "bedsMin": 3,
        "sort": "Price_High_Low",
        "status_type": "ForSale",
    }
    allProps = []
    url = f"{baseUrl}/propertyExtendedSearch"
    cities_list = cities.split(",")
    zpids = []
    for city in cities_list:
        queryWithPage = querystring.copy()
        queryWithPage["location"] = f"{city}, ca"
        pageIdx = 1
        totalPages = 1000
        while pageIdx <= totalPages:
            queryWithPage["page"] = pageIdx
            response = requests.get(url, headers=headers, params=queryWithPage)
            responseJson = response.json()
            props = responseJson["props"]
            for prop in props:
                zpids.append(prop["zpid"])
            totalPages = responseJson["totalPages"]
            pageIdx += 1
    for zpid in zpids:
        propInfo = getDetailByZpid(zpid)
        allProps.append(propInfo)
    formatted_date = datetime.now().strftime("%y%m%d")
    outputFile = f"forsale_{formatted_date}.csv"
    write_to_csv(allProps, outputFile)


def toDateStr(timestamp):
    timestamp = (
        timestamp / 1000
    )  # Convert to seconds since the timestamp is in milliseconds
    dt_object = datetime.fromtimestamp(timestamp)
    return dt_object.strftime("%y%m%d")


def getRecentSoldData(cities, recentDays):
    querystring = {
        "home_type": "Houses",
        "bedsMin": 3,
        "sort": "Price_High_Low",
        "soldInLast": str(recentDays),
        "status_type": "RecentlySold",
    }
    url = f"{baseUrl}/propertyExtendedSearch"
    allProps = []
    cities_list = cities.split(",")
    for city in cities_list:
        queryWithPage = querystring.copy()
        queryWithPage["location"] = f"{city}, ca"
        pageIdx = 1
        totalPages = 1000
        while pageIdx <= totalPages:
            queryWithPage["page"] = pageIdx
            response = requests.get(url, headers=headers, params=queryWithPage)
            # print(response.status_code)
            responseJson = response.json()
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


def main(args):
    cities = args.cities
    statusType = args.status
    recentDays = args.days
    if statusType not in ["ForSale", "RecentlySold"]:
        print(f"Incorrect status: {statusType}")
        sys.exit(-1)
    if statusType == "ForSale":
        getForSaleData(cities)
    else:
        getRecentSoldData(cities, recentDays)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get city house data from Zillow")
    parser.add_argument("--cities", required=True, help="City Name")
    parser.add_argument("--status", default="ForSale", help="[ForSale|RecentlySold]")
    parser.add_argument("--days", default=7, help="recent sold days")
    args = parser.parse_args()
    main(args)