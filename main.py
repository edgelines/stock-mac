import json
import logging
import pymongo
from fastapi import FastAPI, Request, Query, WebSocket, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime, date
# from urllib.request import urlopen, Request
# from typing import Union
from utils import db, send
from Api.post import router as post
from Api.toy import router as toy
from Api.elwApi import router as elw
from Api.indices import router as index
from Api.windowsDB import router as wDB
from Api.themeStock import router as theme
from Api.abc import router as abc
from Api.info import router as info
from Api.formula import router as formula
from Api.industry import router as industry
from Api.themeTop10 import router as themeTop10
from Api.schedule import router as schedule
from Api.stocks import router as stock
from Api.themes import router as themes
from Api.fundamental import router as fundamental
from Api.industryChartData import router as industryChartData
logging.basicConfig(level=logging.INFO)
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
client = pymongo.MongoClient(host=['localhost:27017'])

app.include_router(post, prefix="/post")
app.include_router(toy, prefix="/toy")
app.include_router(wDB, prefix="/api")
app.include_router(stock, prefix="/stockData")
app.include_router(elw, prefix="/api/elwData")
app.include_router(index, prefix="/api/indices")
app.include_router(theme, prefix="/api/theme")
app.include_router(abc, prefix="/api/abc")
app.include_router(info, prefix="/api/info")
app.include_router(formula, prefix="/api/formula")
app.include_router(industry, prefix="/api/industry")
app.include_router(themeTop10, prefix="/api/themeTop10")
app.include_router(schedule, prefix="/api/schedule")
app.include_router(themes, prefix="/api/themes")
app.include_router(fundamental, prefix="/api/fundamental")
app.include_router(industryChartData, prefix="/api/industryChartData")
# app.include_router(post, prefix="/post")
# app.include_router(web, prefix="/web")
# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# app.mount("/aox2", StaticFiles(directory="/Users/checkmate/Work/aox2", html=True), name="aox2")
# # 차단할 IP 주소 목록
BLOCKED_IPS = { "167.248.133.37", "167.248.133.50"}

@app.middleware("http")
async def block_ips(request: Request, call_next):
    client_ip = request.client.host
    result = f"IP : {client_ip}\nURL: {request.url}"
    if client_ip in BLOCKED_IPS:
        send.errors(result, 'Block')
        return Response("Why R U Coimng???", status_code=444)
    elif client_ip.startswith("27.0.238.") :
        send.errors(result, 'Block')
        return Response("Why R U Coimng???", status_code=444)
    return await call_next(request)


# @app.get('/StockData/{code}')
# async def loadStock(code):
# # async def loadStock(code, skip: int=0, limit: int=4000):
#     try :
#         db = client['Stock']
#         data = db[code]
#         mongo_data = list(data.find({}, {'_id':False}))
#         # mongo_data = list(data.find({}, {'_id':False}).skip(skip).limit(limit))
#         return mongo_data
#     except Exception as e:
#         return {"error" : str(e)}
    
# @app.get('/StockSearch/Tracking')
# # ?skip=0&limit=1000
# async def StockSearchTracking(date: date = Query(None), skip: int=0, limit: int=3000):
#     try :
#         db = client['Search']
#         data = db['Tracking']
        
#         query = {}
#         if date :
#             startDay = datetime(date.year, date.month, date.day)
#             endDay = datetime(date.year, date.month, date.day, 23, 59, 59)
#             query['조건일'] = {"$gte": startDay, "$lte": endDay}
#             mongo_data = list(data.find(query, {'_id': False}).skip(skip).limit(limit))
#         else :
#             mongo_data = list(data.find({}, {'_id':False}).skip(skip).limit(limit))

#         return mongo_data
#     except Exception as e:
#         return {"error" : str(e)}
    
# @app.post('/post/StockFinance', response_class=JSONResponse)
# async def StockFinance(request:Request) :
#     req_data = await request.json() # post로 받은 데이터
#     req = json.loads(req_data)
#     db.insertDB('Search','StockFinance',req,'티커')
#     # collection = client['Search']['StockFinance']
#     # collection.delete_many({})
    
#     # if isinstance(req, list):
#     #     collection.insert_many(req)
#     # else:
#     #     collection.insert_one(req)    
#     return {"message" : "Record added successfully"}

