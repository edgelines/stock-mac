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
order ={'_id' :0}

# @router.get("/weather")
# async def Weather( limit : int = 37 ):
#     col = client.Etc.Weather
#     data = pd.DataFrame(col.find({},order).sort('날짜', -1).limit(limit))
#     data = data.sort_values(by='날짜')    
#     return data.to_dict(orient='records')

@router.get("/weather")
async def Weather( limit : int = 37 ):
    col = client.Etc.Weather
    data = pd.DataFrame(col.find({},order).sort('날짜', -1).limit(limit))
    data = data.sort_values(by='날짜')    
    최저, 최고, 날짜 = [], [], []
    눈 = ['눈', '폭설']
    비 = ['비', '뇌우', '소나기']
    요일 = ['월', '화', '수', '목', '금', '토', '일']
    for _, row in data.iterrows():
        if any(keyword in row['예보'] for keyword in 비 ) :
            최고.append({ 'y' : int(row['최고']),  'marker': { 'symbol' : 'url(/icon/rainy.png)' }})
        elif any(keyword in row['예보'] for keyword in 눈) :
            최고.append({ 'y' : int(row['최고']),  'marker': { 'symbol' : 'url(/icon/snow.png)' }})
        else :
            최고.append( int(row['최고']))
        
        최저.append( int(row['최저']))
        날짜.append( f"{row['날짜'].strftime('%m.%d')}<br/>{요일[row['날짜'].weekday()]}" )
    result = {
        '최저' : 최저,
        '최고' : 최고,
        '날짜' : 날짜
    }
    return result


@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['Etc'][name.title()]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})