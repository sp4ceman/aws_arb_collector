import json
import boto3
import datetime as dt
from decimal import Decimal
import pytz
import requests
import collector_functions as f

def lambda_handler(event, context):
    timestamp = dt.datetime.now(pytz.utc)
    
    # define cryptowatch endpoints for exchanges
    kraken = "https://api.cryptowat.ch/markets/kraken/btceur/summary"
    luno = "https://api.cryptowat.ch/markets/luno/btczar/summary"

    # ret responses
    resp_luno = requests.get(luno)
    resp_kraken = requests.get(kraken)
    

    # format responses
    json_luno = f.formatter(resp_luno, timestamp)
    json_kraken = f.formatter(resp_kraken, timestamp)
    # print(json_luno)
    # print(json_kraken)

    
    # # write to dynamo
    dynamodb = boto3.resource('dynamodb')
    
    # table = dynamodb.Table('arbitrator-btc-hist')
    # f.ddb_btc_updater(table, timestamp.replace(second=0, microsecond=0), "luno", json_luno)
    # f.ddb_btc_updater(table, timestamp.replace(second=0, microsecond=0), "kraken", json_kraken)
    
    table = dynamodb.Table('arbitrator-btc-hist-minutely')
    f.ddb_btc_updater_minutely(table, timestamp.replace(second=0, microsecond=0), "luno", json_luno)
    f.ddb_btc_updater_minutely(table, timestamp.replace(second=0, microsecond=0), "kraken", json_kraken)
    
    return {
        'statusCode': 200,
        'body': json.dumps('btc hist table updated')
    }
