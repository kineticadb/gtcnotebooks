#!/usr/bin/env python
import sys
import os

import datetime
import json
import ssl
import time
from urllib.error import URLError, HTTPError
from urllib.request import urlopen

import certifi
import gpudb
import pytz
import websocket
from kinetica_proc import ProcData


login = {
        'event': 'login',
        'data':  {
            'apiKey': "3dadf6bd866be498db6d8ca4233efb0e",
        }
    }
subscribeAll = {
    'event': 'subscribe',
    'data':  {
        'ticker': '*'
    }
}


def get_jsonparsed_data(_url):
    context = ssl.create_default_context(cafile=certifi.where())
    response = urlopen(_url, context=context)
    data = response.read().decode("utf-8")
    return json.loads(data)


def reconnect(webskt):
    counter = 0
    wait = 5
    while counter <= 10:
        try:
            webskt.connect("wss://websockets.financialmodelingprep.com")
            webskt.send(json.dumps(login))
            time.sleep(10)
            webskt.send(json.dumps(subscribeAll))
            return
        except Exception as err:
            print(err)
            time.sleep(wait)
            wait += 5
            counter += 1


if __name__ == '__main__':
    proc_data = ProcData()

    db = gpudb.GPUdb(host=proc_data.request_info['head_url'],
                     username=proc_data.request_info['username'],
                     password=proc_data.request_info['password'])

    schema = [
        ["t", "string", "datetime"],
        ["s", "string", "char8", "dict", "shard_key"],
        ["type", "string", "char4", "dict"],
        ["ap", "float", "nullable"],
        ["as", "int", "nullable"],
        ["bp", "float", "nullable"],
        ["bs", "int", "nullable"],
        ["lp", "float", "nullable"],
        ["ls", "int", "nullable"]
    ]
    tableObj = gpudb.GPUdbTable(
        _type=schema,
        name=f"nyse.prices",
        options={
            "partition_type": "LIST",
            "partition_keys": "date(t)",
            "is_automatic_partition": "true"
        },
        db=db
    )

    djiSchema = [
        ["ts", "string", "datetime"],
        ["symbol", "string", "char16", "dict", "shard_key"],
        ["price", "double"],
        ["volume", "double"]
    ]
    djiTableObj = gpudb.GPUdbTable(
        _type=djiSchema,
        name=f"nyse.index_prices",
        options={
            "partition_type":         "LIST",
            "partition_keys":         "date(ts)",
            "is_automatic_partition": "true"
        },
        db=db
    )

    ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})
    ws.connect("wss://websockets.financialmodelingprep.com")

    print("login")
    ws.send(json.dumps(login))
    time.sleep(10)

    ws.send(json.dumps(subscribeAll))
    url = "https://financialmodelingprep.com/api/v3/quote-short/%5EDJI,%5EGSPC?apikey=3dadf6bd866be498db6d8ca4233efb0e"
    records = []
    indexrecords = []
    timehack = time.time()

    while True:
        if time.time() - timehack >= 1:

            try:
                for quote in get_jsonparsed_data(url):
                    quote["ts"] = datetime.datetime.utcnow().isoformat()
                    indexrecords.append(quote)
                try:
                    djiTableObj.insert_records(indexrecords)
                    indexrecords.clear()
                except gpudb.GPUdbException as gpudberror:
                    print(str(gpudberror))

            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')
                time.sleep(5)
            except URLError as url_err:
                print(f'HTTP error occurred: {url_err}')
                time.sleep(5)

            timehack = time.time()

        try:
            record = ws.recv()

            if ws.connected:
                dictRecord = json.loads(record)
                if 't' in dictRecord:
                    dictRecord['t'] = datetime.datetime.fromtimestamp((dictRecord['t']) / 1000000000.0)\
                        .astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
                    if len(records) > 999:
                        try:
                            tableObj.insert_records(records)
                            records.clear()
                            records.append(dictRecord)

                        except gpudb.GPUdbException as error:
                            print(str(error))

                    else:
                        records.append(dictRecord)
                else:
                    print(datetime.datetime.now())
                    print(json.loads(record))
            else:
                reconnect(ws)

        except websocket._exceptions.WebSocketException as error:
            reconnect(ws)

    proc_data.complete()
