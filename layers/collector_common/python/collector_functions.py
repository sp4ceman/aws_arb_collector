import json
import boto3
import botocore
import pickle
import datetime as dt
import requests
import pytz
from decimal import Decimal
import pandas as pd
import os


def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def formatter(response,timestamp):
    if (response.status_code == 200) :
        req = flatten_json(json.loads(response.content, parse_float=Decimal))
        req["datetime_utc"] = str(timestamp)
    else:
        req = None
    return(req)
    
def ddb_btc_updater(table, ts, exch, dat):
        if dat is not None:
            table.put_item(
            Item={
                    'timestamp_utc': int(ts.timestamp()),
                    'exchange': exch,
                    'data': dat
                }
            )

def ddb_forex_updater(table, ts, source, dat):
        if dat is not None:
            table.put_item(
            Item={
                    'timestamp_utc': int(ts.timestamp()),
                    'source': source,
                    'data': dat
                }
            )

def ddb_btc_updater_minutely(table, ts, exch, dat):
        if dat is not None:
            table.put_item(
            Item={
                    'timestamp_utc': int(ts.timestamp()),
                    'exchange': exch,
                    'content': dat
                }
            )

def ddb_forex_updater_halfhourly(table, ts, source, dat):
        if dat is not None:
            table.put_item(
            Item={
                    'timestamp_utc': int(ts.timestamp()),
                    'source': source,
                    'content': dat
                }
            )

# def s3_csv_writer(bucket, exchange, data, ts):
#     import s3fs
#     filename="data/" + exchange + "/" + str(ts.date()) + "_" + exchange + ".csv"
#     try:
#         boto3.resource('s3').Object("arbitrator-store", filename).load()
#     except botocore.exceptions.ClientError as e:
#         if e.response['Error']['Code'] == "404":
#             # The object does not exist.
#             data.to_csv("s3://" + bucket + "/" + filename, index=False)
#         else:
#             # Something else has gone wrong.
#             raise
#     else:
#         # The object does exist.
#         stemp = pd.read_csv("s3://" + bucket + "/" + filename)
#         stemp = stemp.append(data)
#         stemp.to_csv("s3://" + bucket + "/" + filename, index=False)
    