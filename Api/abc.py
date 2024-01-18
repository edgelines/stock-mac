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

# @router.get("/stockPrice")
# async def StockPrice():
#     try :
#         result = client['ABC']['stockPrice']
#         data = pd.DataFrame(result.find({}, {'_id':0}))
#         data = data.fillna(0)
#         return data.to_dict(orient='records')
#     except Exception as e:
#         return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get("/stockSectorsThemes")
async def StockSectorsThemes():
    try :
        col_테마 = client['Info']['StockThemes']
        테마 = pd.DataFrame(col_테마.find({},{'_id' : 0, '업종명' : 0, '종목코드' : 0}))
        
        db = client['Info']
        collection = db['StockPriceDaily']
        results_df = pd.DataFrame(collection.find({},{'_id' : 0}))
        
        df = pd.merge(results_df, 테마, on='종목명')
        
        return df.to_dict(orient='records')
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['ABC'][name]
        data = pd.DataFrame(result.find({}, {'_id':0}))
        data = data.fillna(0)
        return data.to_dict(orient='records')
        #  list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

# @router.get("/themeBySecByItem")
# async def themeBySecByItem():
#     result = client['ABC']['themeBySecByItem']
#     data = list(result.find({}, {'_id':0}))
#     return data