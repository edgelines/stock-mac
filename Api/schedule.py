from fastapi import APIRouter, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import pymongo
import json
# from pydantic import BaseModel
# from datetime import datetime
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

@router.get("/StockEvent")
async def StockEvent( StockName : str = Query(None) ):
    result = client['Schedule']['StockEvent']
    
    if StockName:
        # 업종명으로 필터링
        data = pd.DataFrame(result.find({}, {'_id': 0}))
        data = data[data['item'] == StockName]
        data = data.to_dict(orient='records')
    else :
        data = list(result.find({}, {'_id':0}))
    return data

@router.get("/ipo")
async def StockIPO( skip: int=0, limit: int=3000 ):
# async def StockIPO( date:date = Query(None), skip: int=0, limit: int=3000 ):
    try :
        result = client['Schedule']['IPO']
        # query = {}
        # if date :
        #     end = datetime(date.year, date.month, date.day)
        #     start = end - timedelta(days=200)
        #     query['사']
        # else :
        data = pd.DataFrame(result.find({}, {'_id':0}).sort([('상장일', -1)]).skip(skip).limit(limit)).to_dict(orient='records')
        return data
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    
@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['Schedule'][name]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})