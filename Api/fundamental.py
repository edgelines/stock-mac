from fastapi import APIRouter, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import pymongo
import json
# from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import numpy as np
import logging
import warnings
import Api.tools as tools
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

# def 날짜전처리(df):
#     df['날짜'] = pd.to_datetime(df['날짜']).astype('int64') // 10**6
#     return df.to_dict(orient='split')['data']

# def 저가지수(origin_df, num, 가격기준) :
#     df = origin_df.copy()
#     df[f'ema{num}'] = ta.EMA(df[가격기준], timeperiod=num)
#     df = df.drop(labels=가격기준, axis=1)
#     df = df.dropna()
#     return tools.날짜전처리(df)

@router.get('/crypto')
async def Crypto():
    try :
        col_Btc = client['Crypto']['Btc']
        col_Eth = client['Crypto']['Eth']
        col_Xrp = client['Crypto']['Xrp']
        Btc = pd.DataFrame(col_Btc.find({},{'_id' : 0}))
        Eth = pd.DataFrame(col_Eth.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))
        Xrp = pd.DataFrame(col_Xrp.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))
        return { 'Btc' : tools.날짜전처리(Btc), 'Eth' : tools.날짜전처리(Eth), 'Xrp' : tools.날짜전처리(Xrp) }
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/moneyIndex')
async def MoneyIndex(ta : str = Query(None)):
    try :
        col_UsdKrw = client['Commodities']['UsdKrw']
        col_UsdEur = client['Commodities']['UsdEur']
        col_UsdCny = client['Commodities']['UsdCny']
        UsdKrw = pd.DataFrame(col_UsdKrw.find({},{'_id' : 0}))
        UsdEur = pd.DataFrame(col_UsdEur.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))
        UsdCny = pd.DataFrame(col_UsdCny.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))

        if ta :
            ema = UsdKrw[['날짜', '저가']].copy()
            가격기준리스트 = [3, 9, 18, 27, 36, 66, 112, 224, 336, 448, 560]
            emaList = []
            for i in 가격기준리스트:
                # name = f'ema{i}'
                df = tools.저가지수(ema, i, '저가')
                emaList.append(df)
            return { 'USD' : tools.날짜전처리(UsdKrw), 'EUR' : tools.날짜전처리(UsdEur), 'CNY' : tools.날짜전처리(UsdCny), 'emaList' : emaList }
        else :
            return { 'USD' : tools.날짜전처리(UsdKrw), 'EUR' : tools.날짜전처리(UsdEur), 'CNY' : tools.날짜전처리(UsdCny) }
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/energy')
async def Energy():
    try :
        col_BrentOil = client['Commodities']['BrentOil']
        col_WtiOil = client['Commodities']['WtiOil']
        BrentOil = pd.DataFrame(col_BrentOil.find({},{'_id' : 0}))
        WtiOil = pd.DataFrame(col_WtiOil.find({},{'_id' : 0 }))
        Wti = WtiOil[['날짜', 'CL=F']]
        Gas = WtiOil[['날짜', 'NG=F']]
        return { 'BrentOil' : tools.날짜전처리(BrentOil), 'Wti' : tools.날짜전처리(Wti), 'Gas' : tools.날짜전처리(Gas) }
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/metals')
async def Metals():
    try :
        col_Gold = client['Commodities']['Gold']
        col_Silver = client['Commodities']['Silver']
        Gold = pd.DataFrame(col_Gold.find({},{'_id' : 0}))
        Silver = pd.DataFrame(col_Silver.find({},{'_id' : 0, '날짜':1, '종가' : 1 }))
        return { 'Gold' : tools.날짜전처리(Gold), 'Silver' : tools.날짜전처리(Silver) }
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/nonMetals')
async def NonMetals():
    try :
        col_Copper = client['Commodities']['Copper']
        col_Aluminium = client['Commodities']['Aluminium']
        Copper = pd.DataFrame(col_Copper.find({},{'_id' : 0, '날짜':1, '종가' : 1}))
        Aluminium = pd.DataFrame(col_Aluminium.find({},{'_id' : 0 , '날짜':1, '종가' : 1}))
        return { 'Copper' : tools.날짜전처리(Copper), 'Aluminium' : tools.날짜전처리(Aluminium) }
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/usdGold')
async def UsdGold():
    try :
        col_Gold = client['Commodities']['Gold']
        col_UsdKrw = client['Commodities']['UsdKrw']
        Gold = pd.DataFrame(col_Gold.find({},{'_id' : 0}))
        UsdKrw = pd.DataFrame(col_UsdKrw.find({},{'_id' : 0 }))
        return { 'Gold' : tools.날짜전처리(Gold), 'UsdKrw' : tools.날짜전처리(UsdKrw) }
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/usdOil')
async def UsdOil():
    try :
        col_BrentOil = client['Commodities']['BrentOil']
        col_UsdKrw = client['Commodities']['UsdKrw']
        BrentOil = pd.DataFrame(col_BrentOil.find({},{'_id' : 0}))
        UsdKrw = pd.DataFrame(col_UsdKrw.find({},{'_id' : 0 }))
        return { 'BrentOil' : tools.날짜전처리(BrentOil), 'UsdKrw' : tools.날짜전처리(UsdKrw) }
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

# @router.get('/{name}')
# async def loadDB(name):
#     try :
#         if name == 'crypto' :
#             col_Btc = client['Crypto']['Btc']
#             col_Eth = client['Crypto']['Eth']
#             col_Xrp = client['Crypto']['Xrp']
#             Btc = pd.DataFrame(col_Btc.find({},{'_id' : 0}))
#             Eth = pd.DataFrame(col_Eth.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))
#             Xrp = pd.DataFrame(col_Xrp.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))
#             Btc['날짜'] = pd.to_datetime(Btc['날짜']).astype('int64') // 10**6
#             Eth['날짜'] = pd.to_datetime(Eth['날짜']).astype('int64') // 10**6
#             Xrp['날짜'] = pd.to_datetime(Xrp['날짜']).astype('int64') // 10**6
#             return { 'Btc' : Btc, 'Eth' : Eth, 'Xrp' : Xrp }
        
#         elif name == 'moneyIndex' :
#             col_UsdKrw = client['Commodities']['UsdKrw']
#             col_UsdEur = client['Commodities']['UsdEur']
#             col_UsdCny = client['Commodities']['UsdCny']
#             UsdKrw = pd.DataFrame(col_UsdKrw.find({},{'_id' : 0}))
#             UsdEur = pd.DataFrame(col_UsdEur.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))
#             UsdCny = pd.DataFrame(col_UsdCny.find({},{'_id' : 0, '날짜' : 1, '종가' : 1}))
#             UsdKrw['날짜'] = pd.to_datetime(UsdKrw['날짜']).astype('int64') // 10**6
#             UsdEur['날짜'] = pd.to_datetime(UsdEur['날짜']).astype('int64') // 10**6
#             UsdCny['날짜'] = pd.to_datetime(UsdCny['날짜']).astype('int64') // 10**6
#             return { 'USD' : UsdKrw, 'EUR' : UsdEur, 'CNY' : UsdCny }
        
#     except Exception as e:
#         logging.error(e)
#         return JSONResponse(status_code=500, content={"message": "Internal Server Error"})