from fastapi import APIRouter, Query
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

@router.get("/Search/IndustryThemes")
async def SearchIndustryThemes( industryName : str = Query(None) ):
    col = client['Info']['IndustryThemes']
    
    if industryName:
        # 업종명으로 필터링
        업종테마=pd.DataFrame(col.find({"업종명" : industryName },{'_id' : 0}))
        # 업종테마 = 업종테마[업종테마['업종명'] == IndustryName ]
        
        col = client.Themes.Rank
        전체테마 = pd.DataFrame(col.find({},{'_id' : 0}))
        
        result = pd.DataFrame()
        for item in 업종테마.테마명.to_list()[0] :
            tmp = 전체테마[전체테마['테마명'] == item]
            result = pd.concat([result, tmp])
        result['순위'] = result.apply( lambda row: f"{row['순위']} ( {row['전일순위'] - row['순위']:+d} )", axis=1 )
        result[['테마명', '등락률', '순위']]
        result['id'] = [i for i in range(len(result.index))]

        # data = pd.DataFrame(col.find({}, {'_id': 0}))
        # data = data[data['업종명'] == IndustryName]
        data = result.to_dict(orient='records')
    else :
        data = list(col.find({}, {'_id':0}))
    return data

@router.get("/Search/StockThemes")
async def SearchStockThemes( stockName : str = Query(None) ):
    col = client.Info.StockThemes
    종목테마=pd.DataFrame(col.find({},{'_id' : 0, '업종명' :0, '종목코드' : 0}))
    
    col = client.Info.StockPriceDaily
    현재가 = pd.DataFrame(col.find({},{'_id' : 0}))
    
    col = client.Themes.Rank
    전체테마 = pd.DataFrame(col.find({},{'_id' : 0}))
    
    result = pd.DataFrame()
    조건 = pd.merge(현재가, 종목테마, on='종목명')
    조건 = 조건[조건['종목명'] == stockName]
    for item in 조건.테마명.to_list()[0] :
        tmp = 전체테마[전체테마['테마명'] == item]
        result = pd.concat([result, tmp])
    result['순위'] = result.apply( lambda row: f"{row['순위']} ( {row['전일순위'] - row['순위']:+d} )", axis=1 )
    result[['테마명', '등락률', '순위']]
    result['id'] = [i for i in range(len(result.index))]
    return result.to_dict(orient='records')

@router.get("/Search/IndustryStocks")
async def SearchIndustryStocks( stockName : str ):
    col = client.Info.IndustryStocks
    df = pd.DataFrame(col.find({"종목명" : stockName },{'_id' : 0}))
    return df.to_dict(orient='records')

@router.get('/stockEtcInfo/{code}')
async def StockEtcInfo(code):
    try :
        col = client['Info']['StockEtcInfo']
        df_base = pd.DataFrame(col.find({"종목코드":code},{'_id' : 0, '시가':0, '종가':0, '변동폭':0, '등락률':0, '거래량':0, '거래대금':0, 'DIV':0,'DPS':0, '5일 평균거래량' :0}))
        
        col = client.Info.Financial
        df_fin = pd.DataFrame(col.find({"종목코드":code},{'_id' : 0}))
        
        col = client.Info.CompanyOverview
        df_com = pd.DataFrame(col.find({"종목코드":code},{'_id' : 0}))
        
        col = client.Info.StockPriceDaily
        df_pri = pd.DataFrame(col.find({"종목코드":code},{'_id' : 0, '종목코드':1, '현재가' : 1}))
        
        col = client.Info.StockThemes
        df_themes = pd.DataFrame(col.find({"종목코드":code},{'_id' : 0, '종목코드':1, '테마명' : 1}))

        col = client['Info']['IpoPulse']
        IpoPulse = pd.DataFrame(col.find({ "종목코드":code },{'_id':0, '종목코드':1, '보호예수' : 1 }))

        # Favorite 
        col = client.Info.Favorite
        favorite = list(col.find({},{'_id':0}))[0]['종목명']
        
        df = df_base.merge(df_fin, on='종목코드').merge(df_com, on='종목코드').merge(df_pri, on='종목코드')
        if '보호예수' in list(IpoPulse.columns) :    
            df = df.merge(IpoPulse, on='종목코드')
        else : 
            df['보호예수'] = ''
        
        if len(df_themes) > 0 :
            df = df.merge(df_themes, on='종목코드')
        else :
            df['테마명'] = ''

        col = client.Schedule.StockEvent
        StockEvent = list(col.find({"종목코드":code},{'_id':0}))
        filtered_data = [item for item in StockEvent if df['종목명'][0] in item['item']]
        if len(filtered_data)>0 :
            _text = ''
            for row in filtered_data :
                _text += f"{row['event']}, " 
            df['이벤트'] = _text[:-2]
        else :
            df['이벤트'] = ''
        
        favo = ['O' for check in favorite if df['종목코드'][0] in check]
        if len(favo)>0 :
            df['Favorite'] = True
        else :
            df['Favorite'] = False
        
        return df.to_dict(orient='records')[0]
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    
@router.get('/Favorite/{code}')
async def StockEtcInfo(code):
    try :
        # Favorite 
        col = client.Info.Favorite
        favorite = list(col.find({},{'_id':0}))[0]['종목명']
        
        if code in favorite :
            favorite.remove(code)
        else :
            favorite.append(code)
        
        result = {'종목명' : favorite}
        col.update_one({"_id": "favorite"}, { "$set": result }, upsert=True )
        

    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})



@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['Info'][name]
        return list(result.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})