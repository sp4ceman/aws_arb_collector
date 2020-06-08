import boto3
from boto3.dynamodb.conditions import Key, Att
import pprint
from decimal import Decimal
import datetime as dt


ddb_resource = boto3.resource('dynamodb')
ddb_client = boto3.client('dynamodb',region_name='eu-west-1')
response = ddb_client.list_tables()
print(response)


# test a range query - looks ok
table = ddb_resource.Table('arbitrator-btc-hist-minutely')
response = table.query(
    KeyConditionExpression=Key('exchange').eq("kraken") & Key('timestamp_utc').between(1586608560-1, 1586608800+1),
    ProjectionExpression='exchange, timestamp_utc, content.result_price_last'
    )
items = response['Items']
pprint.pprint(items)

# Move BTC records between tables
#=================================
# pull all values from old history table
# new_table_min = 1586608560

table = ddb_resource.Table('arbitrator-btc-hist')

response = table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

btc_content = data


len(btc_content)
for d in btc_content:
    d["content"] = d["data"]
    del d["data"]

btc_content[0]

# check the date ranges in the content we've pulled
mini = min([int(btc_content[i]["timestamp_utc"]) for i in range(len(btc_content))])
maxi = max([int(btc_content[i]["timestamp_utc"]) for i in range(len(btc_content))])

print(dt.datetime.utcfromtimestamp(mini).strftime('%Y-%m-%d %H:%M:%S'))
print(dt.datetime.utcfromtimestamp(maxi).strftime('%Y-%m-%d %H:%M:%S'))
# oldest date in the new table
print(dt.datetime.utcfromtimestamp(1586608560).strftime('%Y-%m-%d %H:%M:%S'))

# test batch writer to write data back to new table
table = dynamodb.Table('test')
with table.batch_writer() as batch:
    for i in range(6):
        batch.put_item(
            Item=btc_content[i]
        )

# try write it all in to arbitrator-btc-hist-minutely
table = dynamodb.Table('arbitrator-btc-hist-minutely')
with table.batch_writer() as batch:
    for i in range(len(btc_content)):
        batch.put_item(
            Item=btc_content[i]
        )

# now retrieve all the dates if possible and see if they make sense
table = ddb_resource.Table('arbitrator-btc-hist')
response = table.scan()
date_check = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    date_check.extend(response['Items'])

date_check[0]

unix = [date_check[i]["timestamp_utc"] for i in range(len(date_check))]
len(unix)
dates = [dt.datetime.utcfromtimestamp(i) for i in unix]
min(dates)
max(dates)

dic = {}
for d in dates:

    if str(d.date()) in dic.keys():
        dic[str(d.date())] = dic[str(d.date())] + 1
    else:
        dic[str(d.date())] = 1

pprint.pprint(dic)

import pandas as pd
comp_range = pd.date_range(start=min(dates), end=max(dates), freq="1min")
len(comp_range) * 2 - 18
dic = {}
for d in comp_range:

    if str(d.date()) in dic.keys():
        dic[str(d.date())] = dic[str(d.date())] + 2
    else:
        dic[str(d.date())] = 2

pprint.pprint(dic)


# Move FOREX records between tables
#=================================
# pull all values from old history table
# new_table_min = 1586611800

table = ddb_resource.Table('arbitrator-forex-hist')

response = table.scan()
forex_content = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    forex_content.extend(response['Items'])


len(forex_content)
for d in forex_content:
    d["content"] = d["data"]
    del d["data"]

forex_content[0]

# check the date ranges in the content we've pulled
mini = min([int(forex_content[i]["timestamp_utc"]) for i in range(len(forex_content))])
maxi = max([int(forex_content[i]["timestamp_utc"]) for i in range(len(forex_content))])

print(dt.datetime.utcfromtimestamp(mini).strftime('%Y-%m-%d %H:%M:%S'))
print(dt.datetime.utcfromtimestamp(maxi).strftime('%Y-%m-%d %H:%M:%S'))
# oldest date in the new table
print(dt.datetime.utcfromtimestamp(1586611800).strftime('%Y-%m-%d %H:%M:%S'))

# test batch writer to write data back to new table
table = dynamodb.Table('test')
with table.batch_writer() as batch:
    for i in range(6):
        batch.put_item(
            Item=forex_content[i]
        )

# try write it all in to arbitrator-forex-hist-halfhourly
table = dynamodb.Table('arbitrator-forex-hist-halfhourly')
with table.batch_writer() as batch:
    for i in range(len(forex_content)):
        batch.put_item(
            Item=forex_content[i]
        )

# now retrieve all the dates if possible and see if they make sense
table = ddb_resource.Table('arbitrator-forex-hist-halfhourly')
response = table.scan()
date_check = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    date_check.extend(response['Items'])

date_check[0]

unix = [date_check[i]["timestamp_utc"] for i in range(len(date_check))]
len(unix)
dates = [dt.datetime.utcfromtimestamp(i) for i in unix]
min(dates)
max(dates)

dic = {}
for d in dates:

    if str(d.date()) in dic.keys():
        dic[str(d.date())] = dic[str(d.date())] + 1
    else:
        dic[str(d.date())] = 1

pprint.pprint(dic)

import pandas as pd
comp_range = pd.date_range(start=min(dates), end=max(dates), freq="30min")
len(comp_range) * 2 - 18
dic = {}
for d in comp_range:

    if str(d.date()) in dic.keys():
        dic[str(d.date())] = dic[str(d.date())] + 1
    else:
        dic[str(d.date())] = 1

pprint.pprint(dic)