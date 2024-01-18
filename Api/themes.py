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

@router.get('/rank')
async def themesRank():
    try :
        db = client['Themes']
        col = db['Rank']        
        data = pd.DataFrame(col.find({},{'_id' :0}))
        df = data.fillna(len(data))
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

# @router.get("/themeBySecByItem")
# async def themeBySecByItem():
#     result = client['ABC']['themeBySecByItem']
#     data = list(result.find({}, {'_id':0}))
#     return data