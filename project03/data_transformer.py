import json
import boto3
import random
import datetime
import pandas as pd
import yfinance as yf

kinesis = boto3.client('kinesis', "us-east-2")

stock_tickers = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']
start_date = "2021-11-30"
end_date = "2021-12-01"

def lambda_handler(event, context):
  for ticker in stock_tickers:
    
    data = yf.download(ticker, start=start_date, end=end_date, interval = "5m")
  
    for datetime, row in data.iterrows():
      output = {'name' : ticker}
      output['high'] = round(row['High'], 2)
      output['low'] = round(row['Low'], 2)
      output['ts'] = str(datetime)
      to_json = json.dumps(output)+"\n"
      kinesis.put_record(
                StreamName="sta9760f2021stream1",
                Data=to_json,
                PartitionKey="partitionkey"
                )
