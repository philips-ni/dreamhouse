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
ZILLOW_BASEURL = "https://zillow.com"

HEADERS = {
    "X-RapidAPI-Key": os.environ.get("X_RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
}

MARKED_CONFIG_FILE = "marked.csv"

def writeToCsv(json_content, csv_filename):
    # Opening the file with write permission
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        # If the JSON content is a string,
        # you would need to load it into a Python object.
        if isinstance(json_content, str):
            json_content = json.loads(json_content)
        if json_content:  # Check if json_content is not empty
            # Get the keys from the first dictionary
            # in the list to use as header/column names
            header = json_content[0].keys()
            writer = csv.DictWriter(file, fieldnames=header)
            # Writing the header
            writer.writeheader()
            # Writing the rows
            writer.writerows(json_content)
            print(f"{csv_filename} is created")
        else:
            print("No data to write!")


def getMarkedZpids():
    with open(MARKED_CONFIG_FILE, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        zpids = [int(row["zpid"]) for row in reader]
    return zpids


def getDetailByZpid(zpid):
    time.sleep(1)
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
    pricePerFt = round(price / livingArea, 2)
    listingPrice = -1
    historyEntries = responseJson["priceHistory"]
    pathLink = responseJson["url"]
    link = f"{ZILLOW_BASEURL}/{pathLink}"
    latestEntry = historyEntries[0]
    latestEvent = f"""{latestEntry["date"]} {latestEntry["event"]} """ + \
        f"""{latestEntry["price"]}"""

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
        "link": link,
        "latest_event": latestEvent
    }


def main():
    markedZpids = getMarkedZpids()
    detailInfos = []
    for zpid in markedZpids:
        detailInfo = getDetailByZpid(zpid)
        detailInfos.append(detailInfo)
    formatted_date = datetime.now().strftime("%y%m%d")
    outputFile = f"marked_detail_{formatted_date}.csv"
    writeToCsv(detailInfos, outputFile)


if __name__ == "__main__":
    main()
