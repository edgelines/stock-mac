from fastapi import APIRouter
import json
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
import pandas as pd
import numpy as np
import pymongo
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import JSONResponse

import logging
logging.basicConfig(level=logging.INFO)

router = APIRouter()

client = pymongo.MongoClient(host=['localhost:27017'])

@router.post('/holiday', response_class=JSONResponse)
async def StockSearch(request:Request) :
    req = await request.json() # post로 받은 데이터
    # req = json.loads(req_data)
    # print(req_data)
    collection = client['Search']['Holiday']
    collection.delete_many({})
    
    if isinstance(req, list):
        collection.insert_many(req)
    else:
        collection.insert_one(req)    
    return {"message" : "Record added successfully"}
