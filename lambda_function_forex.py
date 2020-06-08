import json
import boto3
import datetime as dt
from decimal import Decimal
import pytz
import requests
import os
import collector_functions as f

def lambda_handler(event, context):
    timestamp = dt.datetime.now(pytz.utc)
    
    # define forex endpoint
    exch_fixer = "http://data.fixer.io/api/latest?access_key=" + os.environ["fixer"] + "&base=EUR"
    # print(exch_fixer)
    
    # ret responses
    res_exch_fixer = requests.get(exch_fixer)
    # print(res_exch_fixer.content)
    
    # format responses
    json_fixer = json.loads(res_exch_fixer.content, parse_float=Decimal)
    json_fixer["datetime_utc"] = str(timestamp)
    # print(json_fixer)

    # Add a conditional!

    # # write to dynamo
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('arbitrator-forex-hist')
    f.ddb_forex_updater(table, timestamp.replace(second=0, microsecond=0), "fixer", json_fixer)
    
    return {
        'statusCode': 200,
        'body': json.dumps('forex hist table updated')
    }
