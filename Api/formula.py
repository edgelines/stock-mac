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

def process_dataframe(df, column_name, separator_name):
    df = df.drop_duplicates(column_name, keep='first').reset_index(drop=True)
    if column_name == '업종명' or column_name == '종목명' :
        df = df[df['업종명'] != '기타']
    df['구분'] = separator_name
    df = df[[column_name, '구분']]
    df.columns = ['search', 'separator']
    return df

@router.get('/stockSearchInfo')
async def StockSearchInfo():
    try :
        db = client['Info']
        col업종 = db['IndustryStocks']
        col테마 = db['ThemeStocks']
        
        tmp1 = pd.DataFrame(col업종.find({},{'_id' :0}))
        tmp2 = pd.DataFrame(col테마.find({},{'_id' :0}))
        
        themesDF = process_dataframe(tmp2, '테마명', '테마')
        sectorsDF = process_dataframe(tmp1, '업종명', '업종')
        stocksDF = process_dataframe(tmp1, '종목명', '종목')
        data = pd.concat([themesDF, sectorsDF, stocksDF], axis=0).reset_index(drop=True)
        return data.to_dict('records')
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/treasury')
async def Treasury():
    try :
        col_현재가 = client.Info.StockPriceDaily
        주식 = pd.DataFrame(col_현재가.find({},{'_id' : 0, '종목명' :0, '거래량' :0, '전일거래량' :0, '시가' :0, '고가' :0, '저가' :0, '전일대비거래량' : 0}))
        col_자기주식 = client.AoX.TreasuryStock
        자기주식 = pd.DataFrame(col_자기주식.find({},{'_id' : 0}))
        col_기타 = client['Info']['StockEtcInfo']
        기타 = pd.DataFrame(col_기타.find({},{'_id' : 0, '종목코드':1, '시장' :1, 'PBR' :1}))
        col_재무 = client.Info.StockFinance
        재무 = pd.DataFrame(col_재무.find({},{'_id' : 0, '종목명' :0, '시가총액' :0}))
        
        중간정리 = pd.merge(자기주식, 주식, on='종목코드', how='left')
        중간정리.dropna(inplace=True)
        중간정리['수익률'] = 중간정리.apply(lambda x: ((x['현재가'] - x['평균단가']) / x['평균단가'])*100 if x['평균단가'] != 0 else 0, axis=1)
        # 중간정리['총액'] = 중간정리['총액'] / 1000000 #총액 100만원단위
        
        중간정리 = pd.merge(중간정리, 기타, on='종목코드', how='left')
        중간정리 = pd.merge(중간정리, 재무, on='종목코드', how='left')
        중간정리['id'] = 중간정리.index
        중간정리 = 중간정리.fillna(0)
        columns_to_convert = ['평균단가', '수익률', 'PBR']
        for col in columns_to_convert:
            중간정리[col] = round(중간정리[col],1)

        return 중간정리.to_dict('records')
    
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
# @router.get("/themeBySecByItem")
# async def themeBySecByItem():
#     result = client['ABC']['themeBySecByItem']
#     data = list(result.find({}, {'_id':0}))
#     return data