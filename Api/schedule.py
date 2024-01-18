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
from datetime import datetime, timedelta
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

@router.get('/weeks')
async def Weeks( date:str = Query(None)):
    try :
        col = client.Schedule.Weeks
        query ={}
        if date :
            now = datetime.strptime(date, '%Y-%m-%d')
            startDay = now - timedelta(days = now.weekday())
            endDay = startDay + timedelta(days=6)
            query = {'날짜' : {'$gte' : startDay, '$lte':endDay}}
        
        result = pd.DataFrame(col.find(query, {'_id':0}))
        result['날짜'] = pd.to_datetime(result['날짜']).astype('int64') // 10**6
        return result.to_dict(orient='records')
        
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/coex')
async def Coex():
    try :
        col = client.Schedule.Coex
        data = pd.DataFrame(col.find({}, {'_id':False}))
        data = data.sort_values(by='StartDate')
        data['id'] = data.index
        return data.to_dict(orient='records')
    
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['Schedule'][name]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})