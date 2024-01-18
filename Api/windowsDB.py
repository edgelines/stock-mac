import json
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urljoin
import pandas as pd
import numpy as np
import pymongo
from pydantic import BaseModel
from fastapi import FastAPI, Request, Response, WebSocket, HTTPException, Depends, APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.websockets import WebSocketDisconnect
from typing import Any
# from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
# from bson import ObjectId, json_util
from ipaddress import ip_address
import logging
import Api.tools as tools

# from web import router as web
# logging.basicConfig(filename='dummy.log', level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

@router.get('/{name}')
async def loadDB(name):
    try :
        col = client['AoX'][name]
        return list(col.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/Treasury/{name}')
async def load_TreasuryStock(name):
    try :
        col = client['TreasuryStock'][name]
        return list(col.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})


@router.get('/NaverDataLab/{getKey}/{timeUnit}/{name}')
async def naverDataLabDB(getKey, timeUnit, name):
    db = client[f'NaverDataLab_{getKey}_{timeUnit}']
    col = db[name]
    return list(col.find({}, {'_id':False}))

