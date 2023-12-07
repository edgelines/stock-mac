from fastapi import APIRouter
from urllib.request import urlopen, Request
from urllib.parse import urljoin
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi import HTTPException, Depends
from typing import Any
from starlette.responses import JSONResponse
from bson import ObjectId, json_util
import pymongo
from pydantic import BaseModel
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

# toy
@router.get('/{name}')
async def toyGet(name):
    try :
        collection = client['toy'][name]
        cursor = collection.find({})  # _id 포함
        # ObjectId를 문자열로 변환
        # result = json.loads(json_util.dumps(cursor))
        # return result
        all_comments = []
        for comment in cursor:
            comment['_id'] = str(comment['_id'])
            all_comments.append(comment)
        return all_comments
        # return {"result": all_comments}
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.post('/uploadCoffee', response_class=JSONResponse)
async def Thyroglobulin(request:Request) :
    req = await request.json() # post로 받은 데이터
    collection = client['toy']['Coffee']
    collection.insert_one(req)
    return {"message" : "Record added successfully"}

@router.put('/updateCoffee/{id}', response_class=JSONResponse)
async def toyUpdate(id: str, request: Request):
    # print(f"Received id: {id}")  # _id 값 로깅
    req = await request.json()
    collection = client['toy']['Coffee']
    
    # ObjectId로 변환
    obj_id = ObjectId(id)
    # print(f"Converted to ObjectId: {obj_id}")  # 변환된 ObjectId 값 로깅
    
    update_result = collection.update_one({"_id": obj_id}, {"$set": req})
    # print(f"Update Result: {update_result.raw_result}")  # 수정 결과 로깅
    
    if update_result.modified_count:
        return {"message": "Record updated successfully"}
    else:
        raise HTTPException(status_code=404, detail="Item not found")


@router.delete('/deleteCoffee/{id}', response_class=JSONResponse)
async def toyDelete(id: str):
    collection = client['toy']['Coffee']
    
    # ObjectId로 변환
    obj_id = ObjectId(id)

    delete_result = collection.delete_one({"_id": obj_id})
    
    if delete_result.deleted_count:
        return {"message": "Record deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Item not found")
    
class Record(BaseModel):
    date: datetime
    thyroglobulin: float
    thyroglobulin_ab: float
    TSH : float
        
@router.post('/thyroglobulin', response_class=JSONResponse)
async def Thyroglobulin(record:Record) :
    collection = client['Kakao']['Thyroglobulin']
    record_dict = record.dict()
    collection.insert_one(record_dict)
    return {"message" : "Record added successfully"}

@router.get("/thyroglobulin")
async def get_records():
    collection = client['Kakao']['Thyroglobulin']
    return list(collection.find({}, {'_id':False}))
    # return {"records": records}
