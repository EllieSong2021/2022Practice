import argparse
import json
import os
import sys

from sodapy import Socrata

import requests
from requests.auth import HTTPBasicAuth

DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
INDEX_NAME = os.environ["INDEX_NAME"]
ES_HOST = os.environ["ES_HOST"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

parser = argparse.ArgumentParser(description="Process data from violation.")
parser.add_argument("--page_size", type=int, help="How many rows to fetch per page", required=True)
parser.add_argument("--num_pages", type=int, help="How many pages to fetch")
args = parser.parse_args(sys.argv[1:])
print(args)

if __name__ == '__main__':
    try: 
        resp = requests.put(
            f"{ES_HOST}/{INDEX_NAME}", 
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "plate": {"type": "text"},
                        "state": {"type": "text"},
                        "license_type": {"type": "text"},
                        "summons_number": {"type": "text"},
                        "issue_date": {"type": "date", "format": "MM/dd/yyyy"},
                        "violation": {"type": "keyword"},
                        "fine_amount": {"type": "float"},
                        "penalty_amount": {"type": "float"},
                        "interest_amount": {"type": "float"},
                        "reduction_amount": {"type": "float"},
                        "payment_amount": {"type": "float"},
                        "amount_due": {"type": "float"},
                        "precinct": {"type": "keyword"},
                        "county": {"type": "keyword"},
                    }
                }
            }
        )
        resp.raise_for_status()
        print("done")
    except Exception as e:
        print("Index already exists!")
        
    client = Socrata(
        "data.cityofnewyork.us",
        APP_TOKEN,
    )
    
    rows = client.get(DATASET_ID, limit=args.page_size)
    es_rows = []
    for row in rows:
        try:
            #we need to translate our row into a dictionary that properly...
            #...encodes the data as we defined it
            es_row = {}
            es_row["plate"] = row["plate"]
            es_row["state"] = row["state"]
            es_row["license_type"] = row["license_type"]
            es_row["summons_number"] = row["summons_number"]
            es_row["issue_date"] = row["issue_date"]
            es_row["violation"] = row["violation"]
            es_row["fine_amount"] = float(row["fine_amount"])
            es_row["penalty_amount"] = float(row["penalty_amount"])
            es_row["interest_amount"] = float(row["interest_amount"])
            es_row["reduction_amount"] = float(row["reduction_amount"])
            es_row["payment_amount"] = float(row["payment_amount"])
            es_row["amount_due"] = float(row["amount_due"])
            es_row["precinct"] = row["precinct"]
            es_row["county"] = row["county"]

        except Exception as e:
            print(f"FAILED! error is: {e}")
            continue #If here, we want to short circuit this loop
                     #this means we want to go back to the top of the for loop here

        #this will populate a list of es_rows
        es_rows.append(es_row)

    #print(es_rows)
    #print(len(es_rows))
    """
    This is example of how it should look
    {"index": {"_index": "your_index", "_type": "your_type", "_id": "975463943"}}
    {"Amount": "480", "Quantity": "2", "Id": "975463711", "Client_Store_sk": "1109"}
    {"index": {"_index": "your_index", "_type": "your_type", "_id": "975463943"}}
    {"Amount": "2105", "Quantity": "2", "Id": "975463943", "Client_Store_sk": "1109"}
    """

    bulk_upload_data = ""
    for i, line in enumerate(es_rows):
        print(f"Handling row {line['summons_number']} {i}")
        action = '{"index": {"_index": "'+INDEX_NAME+'", "_type" : "_doc"}}'
        data = json.dumps(line)
        bulk_upload_data += f"{action}\n"
        bulk_upload_data += f"{data}\n"

    print(bulk_upload_data)

    #here we will push to elasticsearch
    try:
        resp = requests.post(
            f"{ES_HOST}/_bulk",
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            data=bulk_upload_data,
            headers={
                "Content-Type": "application/x-ndjson"
            }
        )
        resp.raise_for_status()
        print("done")
    except Exception as e:
        print(f"Failed to upload to elasticsearch! {e}")
                   





   
