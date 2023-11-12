import json
import logging
import pymongo
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
# from urllib.request import urlopen, Request
# from typing import Union
from utils import db

logging.basicConfig(level=logging.INFO)
app = FastAPI()
client = pymongo.MongoClient(host=['localhost:27017'])
# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/StockData/{code}')
async def loadStock(code):
    try :
        db = client['Stock']
        data = db[code]
        mongo_data = list(data.find({}, {'_id':False}))
        return mongo_data
    except Exception as e:
        return {"error" : str(e)}
    
@app.get('/StockSearch/Tracking')
# ?skip=0&limit=1000
async def StockSearchTracking(skip: int=0, limit: int=2000):
    try :
        db = client['Search']
        data = db['Tracking']
        mongo_data = list(data.find({}, {'_id':False}).skip(skip).limit(limit))
        return mongo_data
    except Exception as e:
        return {"error" : str(e)}
    
@app.post('/post/StockFinance', response_class=JSONResponse)
async def StockFinance(request:Request) :
    req_data = await request.json() # post로 받은 데이터
    req = json.loads(req_data)
    db.insertDB('Search','StockFinance',req,'티커')
    # collection = client['Search']['StockFinance']
    # collection.delete_many({})
    
    # if isinstance(req, list):
    #     collection.insert_many(req)
    # else:
    #     collection.insert_one(req)    
    return {"message" : "Record added successfully"}

