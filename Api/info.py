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

@router.get("/Search/IndustryThemes")
async def SearchIndustryThemes( IndustryName : str = Query(None) ):
    result = client['Info']['IndustryThemes']
    
    if IndustryName:
        # 업종명으로 필터링
        data = pd.DataFrame(result.find({}, {'_id': 0}))
        data = data[data['업종명'] == IndustryName]
        data = data.to_dict(orient='records')
    else :
        data = list(result.find({}, {'_id':0}))
    return data

@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['Info'][name]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})