from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import pymongo
import json
import pandas as pd
import numpy as np
import logging
import talib as ta
from datetime import datetime
import Api.tools as tools
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])


    

def getADR(df, num, current_index):
    if current_index < num:  # num 이전의 데이터가 충분하지 않을 때
        return None  # None 반환
    # num 만큼 이전 데이터를 사용하여 ADR 계산
    sub_df = df.iloc[current_index-num+1:current_index+1]
    advance = sub_df['상승'].sum()
    decline = sub_df['total'].sum() - advance
    if decline == 0:
        return None  # 0으로 나눌 수 없으므로 None 반환
    return (advance / decline) * 100

@router.get('/adr')
async def ADR(num:int, dbName:str):
    try :
        col = client.ALL[dbName]
        # col = client.ALL.MarketKospi200
        df = pd.DataFrame(col.find({},{'_id' : 0, '상승%' : 0}).sort('날짜', -1).limit(1200))
        df['날짜'] = pd.to_datetime(df['날짜'], format='%Y-%m-%d')
        df.sort_values(by='날짜', inplace=True)
        df = df.reset_index(drop=True)
        
        df['ADR'] = df.apply(lambda row : getADR(df, num, row.name), axis=1)
        df = df[['날짜', 'ADR']].dropna()
        return tools.날짜전처리(df)

    except Exception as e :
        logging.error(2)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    
@router.get('/willr')
async def WillR(num:int, dbName:str):
    try :
        col = client.ALL[dbName]
        # col = client.ALL.Kospi200
        df = pd.DataFrame(col.find({},{'_id' : 0, '거래량' : 0, '거래대금' : 0}).sort('날짜', -1).limit(1200))
        df['날짜'] = pd.to_datetime(df['날짜'])
        df.sort_values(by='날짜', inplace=True)
        df = df.reset_index(drop=True)
        df['WillR'] = ta.WILLR(df['고가'], df['저가'], df['종가'], timeperiod=num)
        df = df.dropna()
        return tools.날짜전처리(df[['날짜', 'WillR']])
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    
@router.get('/{name}')
async def Kospi200(name):
    try :
        col = client.ALL[name]
        # data = pd.DataFrame(col.find({},{'_id' : 0, '거래량' : 0, '거래대금' : 0}).sort('날짜', -1).limit(1200))
        df = pd.DataFrame(col.find({},{'_id' : 0, '거래량' : 0, '거래대금' : 0}).sort('날짜', -1).limit(1200))
        
        # col = client.GPO.StartDate
        # res = list(col.find({},{'_id':0}))
        # 간지 = pd.DataFrame(res[1])
        # 간지 = 간지[간지['날짜'] > datetime.today()].head(10)
        # df = pd.concat([data, 간지]).fillna(0)
        df['날짜'] = pd.to_datetime(df['날짜'])
        
        df.sort_values(by='날짜', inplace=True)
        df = df.reset_index(drop=True)
        df = df[['날짜', '시가', '고가', '저가', '종가']]
        # result = {
        #     'data' : tools.날짜전처리(df),
        #     'min': data['저가'].min()
        # }
        return tools.날짜전처리(df)
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})