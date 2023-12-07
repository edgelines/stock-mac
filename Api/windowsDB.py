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

# from web import router as web
# logging.basicConfig(filename='dummy.log', level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['AoX'][name]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/ALL/{name}')
async def load_ALL_DB(name):
    try :
        result = client['ALL'][name]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/Treasury/{name}')
async def load_TreasuryStock(name):
    try :
        result = client['TreasuryStock'][name]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

# @router.get('/json/{name}')
# async def loadJson(name):
#     file_path = 'D:/Data/' + str(name) + '.json'
#     with open(file_path, encoding='utf-8') as json_file:
#         data = json.load(json_file)
#     return data

@router.get('/VixMA')
async def VixMA():
    db = client['AoX']
    result = db['Vix']
    return list(result.find({}, {'_id':False}))[-200:]
    # Vix = pd.DataFrame.from_records(result.find({}, {'_id':False}))

# @router.get('/exPast/{name}/{num}')
# async def exPast(name, num):
#     file_path = 'D:/CheckMate/data/exPast_' + str(name)+str(num) + '.json'
#     with open(file_path, encoding='utf-8') as json_file:
#         data = json.load(json_file)
#     return data

@router.get('/NaverDataLab/{getKey}/{timeUnit}/{name}')
async def naverDataLabDB(getKey, timeUnit, name):
    db = client[f'NaverDataLab_{getKey}_{timeUnit}']
    result = db[name]
    return list(result.find({}, {'_id':False}))

# @router.get("/image/{file_path:path}")
# async def read_image(file_path: str):
#     return FileResponse('D:/Data/' + file_path+'.jpg')

# @router.get("/icon/{file_path:path}")
# async def read_image(file_path: str):
#     return FileResponse('D:/Data/icon/' + file_path+'.png')

