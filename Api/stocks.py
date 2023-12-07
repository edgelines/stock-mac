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

@router.get('/{code}')
async def loadStock(code):
# async def loadStock(code, skip: int=0, limit: int=4000):
    try :
        db = client['Stocks']
        data = db[code]

        df = pd.DataFrame(data.find({}, {'_id':False}))
        df['날짜'] = pd.to_datetime(df['날짜']).astype('int64') // 10**6
        # mongo_data = list(data.find({}, {'_id':False}).skip(skip).limit(limit))
        stock, volume = [], []
        for item in df.to_dict(orient='records') :
            stock.append([item['날짜'], item['시가'],item['고가'],item['저가'],item['종가']])
            volume.append([item['날짜'], item['거래량']])
        result = { 'stock' : stock, 'volume' : volume }
        return result

        # return mongo_data
    except Exception as e:
        return {"error" : str(e)}