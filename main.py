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
from Api.toy import router as toy
from Api.elwApi import router as elw
from Api.indices import router as index
from Api.windowsDB import router as wDB
from Api.themeStock import router as theme
from Api.abc import router as abc
from Api.info import router as info
from Api.formula import router as formula
from Api.industry import router as industry
# from Api.themeTop10 import router as themeTop10
from Api.schedule import router as schedule
from Api.stocks import router as stock
from Api.themes import router as themes
from Api.fundamental import router as fundamental
from Api.industryChartData import router as industryChartData
from Api.modeling import router as modeling
from Api.etc import router as etc
from Api.aox import router as aox
from Api.hts import router as hts
from Api.portfolio import router as portfolio
logging.basicConfig(level=logging.INFO)
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
client = pymongo.MongoClient(host=['localhost:27017'])

app.include_router(toy, prefix="/toy")
app.include_router(wDB, prefix="/api")
app.include_router(stock, prefix="/stockData")
# /api
app.include_router(abc, prefix="/api/abc")
app.include_router(aox, prefix="/api/aox")
app.include_router(elw, prefix="/api/elwData")
app.include_router(etc, prefix="/api/etc")
app.include_router(formula, prefix="/api/formula")
app.include_router(fundamental, prefix="/api/fundamental")
app.include_router(hts, prefix="/api/hts")

app.include_router(index, prefix="/api/indices")
app.include_router(info, prefix="/api/info")
app.include_router(industry, prefix="/api/industry")
app.include_router(industryChartData, prefix="/api/industryChartData")

app.include_router(schedule, prefix="/api/schedule")
app.include_router(modeling, prefix="/api/modeling")

app.include_router(theme, prefix="/api/theme")
app.include_router(themes, prefix="/api/themes")
# app.include_router(themeTop10, prefix="/api/themeTop10")
# app.include_router(post, prefix="/post")
# app.include_router(web, prefix="/web")
# app.include_router(portfolio, prefix="/trading")
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
        send.check_logs(result)
        return Response("...", status_code=500)

    # censy
    elif client_ip.startswith("167.248.133."):
        send.check_logs(result)
        return Response("...", status_code=500)
    
    # 성남 카카오 추측
    elif client_ip.startswith("211.231.103."):
        send.check_logs(result)
        return Response("...", status_code=500)
    # 제주 카카오
    elif client_ip.startswith("27.0."):
        segments = client_ip.split(".")
        if len(segments) >= 3 and 236 <= int(segments[2]) <= 239:
            send.check_logs(result)
            return Response("...", status_code=500)
    
    # 중국
    elif client_ip.startswith("211.149."):
        segments = client_ip.split(".")
        if len(segments) >= 3 and 128 <= int(segments[2]) <= 255:
            send.check_logs(result)
            return Response("...", status_code=500)
        
    return await call_next(request)
    
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

