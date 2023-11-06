import asyncio
import json
import sys
from asyncio import current_task
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from urllib.parse import urljoin
import pandas as pd
import pymongo
from pydantic import BaseModel
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.websockets import WebSocketDisconnect
from fastapi import FastAPI, HTTPException, Depends
from typing import Any
from starlette.responses import JSONResponse
from bson import ObjectId, json_util
import logging
from typing import Union
from fastapi import FastAPI
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