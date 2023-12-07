from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
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

@router.get("/lowSectorsRankDfTop3")
async def lowSectorsRankDfTop3():
    result = client['AoX']['lowSectorsRankDfTop3']
    data = pd.DataFrame(result.find({}, {'_id':0}).sort([('날짜', -1)]).limit(3))
    data=data.sort_values(by='날짜')
    data = data.to_dict(orient='records')
    return data