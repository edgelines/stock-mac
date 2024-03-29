from fastapi import APIRouter
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

@router.get('/stockSectors')
async def stockSectors():
    try :
        db = client['Industry']
        col = db['Rank']        
        data = list(col.find({},{'_id' :0, '전체' : 0, '상승' : 0, '보합' : 0, '하락' : 0, '등락그래프' : 0, '상승%' : 0, '순위' : 0}))
        # return data
        return sorted(data, key=lambda x: x['전일대비'], reverse=True )
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/LowRankTableTop3')
async def LowRankTableTop3():
    try :
        col = client['Industry']['LowRankTableTop3']
        data = pd.DataFrame(col.find({}, {'_id':False}).sort([('날짜', -1)]).limit(3))
        data=data.sort_values(by='날짜')
        return data.to_dict(orient='records')
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/{name}')
async def loadDB(name):
    try :
        col = client['Industry'][name]
        return list(col.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

# @router.get("/themeBySecByItem")
# async def themeBySecByItem():
#     result = client['ABC']['themeBySecByItem']
#     data = list(result.find({}, {'_id':0}))
#     return data