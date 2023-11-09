import pymongo
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from typing import Union

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
    
