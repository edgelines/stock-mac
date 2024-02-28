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

def CPI전처리(collecton, columns={'_id':0, '날짜':1, 'YoY' :1}):
    df = pd.DataFrame(collecton.find({}, columns))
    df = df[['날짜', 'YoY']]
    # df.rename(columns={'date' :'날짜'}, inplace=True)
    return tools.날짜전처리(df)

def CPI최근값(collecton, columns={'_id':0, 'year':1, 'month':1, '날짜':1, 'YoY' :1}):
    df = pd.DataFrame(collecton.find({}, columns).sort('날짜', -1).limit(6) )
    df = df.sort_values(by='날짜')[['year', 'month', 'YoY']]
    df['전월대비'] = df['YoY'].diff().fillna(0)  # 첫 번째 행에 대해서는 차이를 계산할 수 없으므로 0으로 채웁니다.
    df = df[1:]
    df['전월대비'] = df['전월대비'].apply(lambda x: round(x, 2))
    return df.to_dict(orient='records')

name_mappings = {
            # Main
            'CUUR0000SA0': 'CPI',
            'CUUR0000SAF1': 'Foods',
            'CUUR0000SA0E': 'Energy',
            'CUUR0000SACL1E': 'Commodities',
            'CUUR0000SASLE': 'Services',
            
            # Foods
            'CUUR0000SAF111' : 'Cereals',
            'CUUR0000SAF112' : 'Meats',
            'CUUR0000SEFJ' : 'Dairy',
            'CUUR0000SAF113' : 'Fruits',
            'CUUR0000SAF114' : 'Non Alcoholic',
            'CUUR0000SAF115' : 'Other',
            'CUUR0000SEFV' : 'Food Away',
            
            # Energy
            'CUUR0000SEHE01' : 'Fuel',
            'CUUR0000SETB01' : 'Gasoline',
            'CUUR0000SEHF01' : 'Electricity',
            'CUUR0000SEHF02' : 'Natural Gas',
            
            #Commodities
            'CUUR0000SAA' : 'Apparel', 
            'CUUR0000SETA01' : 'New Vehicles', 
            'CUUR0000SETA02' : 'Used Car', 
            'CUUR0000SAM1' : 'Medical Care', 
            'CUUR0000SAF116' : 'Alcoholic', 
            'CUUR0000SEGA' : 'Tobacco', 
            
            # Services
            'CUUR0000SAH1' : 'Shelter',
            'CUUR0000SAM2' : 'Medical Care Services',
            'CUUR0000SETD' : 'Motor Maintenance',
            'CUUR0000SETE' : 'Motor Insurance',
            'CUUR0000SETG01' : 'Airline Fare',

            # PPI
            'WPUFD4' : 'PPI',
            'MPU0011013' : 'Agriculture',
            'MPU0021013' : 'Mining',
            'MPU0022013' : 'Utilities',
            'MPU0023013' : 'Construction',
            'MPU9900013' : 'Manufacturing',
            'MPU9920013' : 'Durable Manufacturing',
            'MPU9910013' : 'Nondurable Manufacturing',
            'MPU0042013' : 'Trade',
            'MPU0048013' : 'Transportation and warehousing',
            'MPU0051013' : 'Information',
            'MPU5253013' : 'Finance, insurance, and real estate',
            'MPU5481013' : 'Services',

            # TFP Combined Inputs
            'MPU0021033':'Mining',
            'MPU0022033':'Utilities',
            'MPU0023033':'Construction',
            'MPU9900033':'Manufacturing',
            'MPU9920033':'Durable Manufacturing',
            'MPU9910033':'Nondurable Manufacturing',
            'MPU0011033':'Agriculture',
            'MPU4244033':'Trade',
            'MPU0048033':'Transportation and warehousing',
            'MPU0051033':'Information',
            'MPU5253033':'Finance, insurance, and real estate',
            'MPU5481033':'Services',
            # TFP output
            'MPU0021513' :'Mining',
            'MPU0022513' :'Utilities',
            'MPU0023513' :'Construction',
            'MPU9900513' :'Manufacturing',
            'MPU9900513' :'Durable Manufacturing',
            'MPU9910513' :'Nondurable Manufacturing',
            'MPU0011513' :'Agriculture',
            'MPU4244513' :'Trade',
            'MPU0048513' :'Transportation and warehousing',
            'MPU0051513' :'Information',
            'MPU5253513' :'Finance, insurance, and real estate',
            'MPU5481513' :'Services',
        }
# 각 CPI 종류별 MongoDB 컬렉션 정의
collections = {
    'ALL': ['CUUR0000SA0',
            'CUUR0000SAF1', 'CUUR0000SAF111', 'CUUR0000SAF112', 'CUUR0000SEFJ', 'CUUR0000SAF113', 'CUUR0000SAF114', 'CUUR0000SAF115', 'CUUR0000SEFV',
            'CUUR0000SA0E','CUUR0000SEHE01', 'CUUR0000SETB01', 'CUUR0000SEHF01', 'CUUR0000SEHF02',
            'CUUR0000SACL1E','CUUR0000SAA', 'CUUR0000SETA01', 'CUUR0000SETA02', 'CUUR0000SAM1', 'CUUR0000SAF116', 'CUUR0000SEGA',
            'CUUR0000SASLE','CUUR0000SAH1', 'CUUR0000SAM2', 'CUUR0000SETD', 'CUUR0000SETE', 'CUUR0000SETG01'],
    'CPI': ['CUUR0000SA0', 'CUUR0000SAF1', 'CUUR0000SA0E', 'CUUR0000SACL1E', 'CUUR0000SASLE'],
    'Foods': ['CUUR0000SAF1', 'CUUR0000SAF111', 'CUUR0000SAF112', 'CUUR0000SEFJ', 'CUUR0000SAF113', 'CUUR0000SAF114', 'CUUR0000SAF115', 'CUUR0000SEFV'],
    'Energy': ['CUUR0000SA0E','CUUR0000SEHE01', 'CUUR0000SETB01', 'CUUR0000SEHF01', 'CUUR0000SEHF02'],
    'Commodities': ['CUUR0000SACL1E','CUUR0000SAA', 'CUUR0000SETA01', 'CUUR0000SETA02', 'CUUR0000SAM1', 'CUUR0000SAF116', 'CUUR0000SEGA'],
    'Services': ['CUUR0000SASLE','CUUR0000SAH1', 'CUUR0000SAM2', 'CUUR0000SETD', 'CUUR0000SETE', 'CUUR0000SETG01'],

    # Foods
    'Cereals' : ['CUUR0000SAF1','CUUR0000SAF111'],
    'Meats' : ['CUUR0000SAF1','CUUR0000SAF112'],
    'Dairy' : ['CUUR0000SAF1','CUUR0000SEFJ'],
    'Fruits' : ['CUUR0000SAF1','CUUR0000SAF113'],
    'Non Alcoholic' : ['CUUR0000SAF1','CUUR0000SAF114'],
    'Other' : ['CUUR0000SAF1','CUUR0000SAF115'],
    'Food Away' : ['CUUR0000SAF1','CUUR0000SEFV'],

    # Energy
    'Fuel' : ['CUUR0000SA0E', 'CUUR0000SEHE01'],
    'Gasoline' : ['CUUR0000SA0E', 'CUUR0000SETB01'],
    'Electricity' : ['CUUR0000SA0E', 'CUUR0000SEHF01'],
    'Natural Gas' : ['CUUR0000SA0E', 'CUUR0000SEHF02'],

    #Commodities
    'Apparel' : ['CUUR0000SACL1E', 'CUUR0000SAA'],
    'New Vehicles' : ['CUUR0000SACL1E', 'CUUR0000SETA01'],
    'Used Car' : ['CUUR0000SACL1E', 'CUUR0000SETA02'],
    'Medical Care' : ['CUUR0000SACL1E', 'CUUR0000SAM1'],
    'Alcoholic' : ['CUUR0000SACL1E', 'CUUR0000SAF116'],
    'Tobacco' : ['CUUR0000SACL1E', 'CUUR0000SEGA'],

    # Services
    'Shelter' : ['CUUR0000SASLE','CUUR0000SAH1'],
    'Medical Care Services' : ['CUUR0000SASLE','CUUR0000SAM2'],
    'Motor Maintenance' : ['CUUR0000SASLE','CUUR0000SETD'],
    'Motor Insurance' : ['CUUR0000SASLE','CUUR0000SETE'],
    'Airline Fare' : ['CUUR0000SASLE','CUUR0000SETG01'],

    'PPI': ['CUUR0000SA0', 'WPUFD4'],
    'TFP1' : ['MPU0021013', 'MPU0022013', 'MPU0023013', 'MPU9900013', 'MPU9920013', 'MPU9910013'],
    'TFP2' : ['MPU0011013', 'MPU0042013', 'MPU0048013', 'MPU0051013', 'MPU5253013', 'MPU5481013']
    # 'Agriculture' : ['MPU0011013', 'MPU0021013', 'MPU0023013', 'MPU9900013', 'MPU9920013', 'MPU9910013'],
    # 'Utilities' : ['MPU0022013', 'MPU0042013', 'MPU0048013', 'MPU0051013', 'MPU5253013', 'MPU5481013']
}

@router.get('/blsGov')
async def getBlsGov(name:str):
    try:
        result = {}
        for category_code in collections.get(name, []):
            col = client.BlsGov[category_code]
            category_name = name_mappings.get(category_code, category_code)
            if name == category_name:
                chartType = 'line'
            else : chartType = 'column'
            
            if name == 'PPI' :
                result[category_name] = {
                'name': category_name,
                'data': CPI전처리(col),
                'type': 'line'
            } 
            
            else :
                result[category_name] = {
                'name': category_name,
                'data': CPI전처리(col),
                'type': chartType
            }
        return result
    
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/CPIvalue')
async def getCPI(name:str):
    try:
        result = []
        for category_code in collections.get(name, []):
            col = client.BlsGov[category_code]
            category_name = name_mappings.get(category_code, category_code)
            result.append({ category_name : CPI최근값(col) })

        return result
    
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

def getYoY_Data(lit : list ):
    tmp =[]
    for name in lit :
        collection = client.BlsGov[name]
        data = list(collection.find({},{'_id':0}).sort('날짜',-1).limit(1))[0]
        tmp.append(data['YoY'])
    return tmp

@router.get('/TFP')
async def getTFP():

    try:
        TFP = ['MPU0021013', 'MPU0022013', 'MPU0023013', 'MPU9900013', 'MPU9920013', 'MPU9910013', 'MPU0011013', 'MPU0042013', 'MPU0048013', 'MPU0051013', 'MPU5253013', 'MPU5481013']
        CombinedInputs = ['MPU0021033', 'MPU0022033', 'MPU0023033', 'MPU9900033', 'MPU9920033', 'MPU9910033', 'MPU0011033', 'MPU4244033', 'MPU0048033', 'MPU0051033', 'MPU5253033', 'MPU5481033']
        Output = ['MPU0021513', 'MPU0022513', 'MPU0023513', 'MPU9900513', 'MPU9900513', 'MPU9910513', 'MPU0011513', 'MPU4244513', 'MPU0048513', 'MPU0051513', 'MPU5253513', 'MPU5481513']
        result = {}
        
        result['TFP'] = { 'data' : getYoY_Data(TFP), 'type' : 'bar'}
        result['CombinedInputs'] = {'data' : getYoY_Data(CombinedInputs), 'type' : 'bar'}
        result['Output'] = {'data':getYoY_Data(Output), 'type' :'line' }

        return result
    
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/nextReleaseDate')
async def getNextReleaseDate():
    오늘 = datetime.now()
    col = client.BlsGov.Schedule
    qurey = {'날짜':{'$gt' : 오늘}}
    projection = {'_id':0, '날짜':1}
    day = list(col.find(qurey, projection))[0]['날짜']
    return day

@router.get('/cpi')
async def CPI():
    try:
        col = client.Fundamental.Cpi
        df = pd.DataFrame(col.find({},{'_id' :0}))
        return tools.날짜전처리(df)
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    
@router.get('/ppi')
async def PPI():
    try:
        col = client.Fundamental.Ppi
        df = pd.DataFrame(col.find({},{'_id' :0}))
        PCUOMINOMIN = df[['날짜', 'PCUOMINOMIN']]
        MEDCPIM158SFRBCLE = df[['날짜', 'MEDCPIM158SFRBCLE']]
        result = {
            'PCUOMINOMIN' : tools.날짜전처리(PCUOMINOMIN),
            'MEDCPIM158SFRBCLE' : tools.날짜전처리(MEDCPIM158SFRBCLE),
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    
@router.get('/inventories')
async def Inventories():
    try:
        col = client.Fundamental.Inventories
        df = pd.DataFrame(col.find({},{'_id' :0}))
        ISRATIO = df[['날짜', 'ISRATIO']]
        RETAILIMSA = df[['날짜', 'RETAILIMSA']]
        RETAILIRSA = df[['날짜', 'RETAILIRSA']]
        result = {
            'ISRATIO' : tools.날짜전처리(ISRATIO),
            'RETAILIMSA' : tools.날짜전처리(RETAILIMSA),
            'RETAILIRSA' : tools.날짜전처리(RETAILIRSA)
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/bond')
async def Bond():
    try:
        col = client.Fundamental.Bond
        df = pd.DataFrame(col.find({},{'_id' :0}))
        T10Y2Y = df[['날짜', 'T10Y2Y']]
        T1YFF = df[['날짜', 'T1YFF']]
        T5YFF = df[['날짜', 'T5YFF']]
        result = {
            'T10Y2Y' : tools.날짜전처리(T10Y2Y),
            'T1YFF' : tools.날짜전처리(T1YFF),
            'T5YFF' : tools.날짜전처리(T5YFF)
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/mortgage')
async def Mortgage():
    try:
        col = client.Fundamental.Mortgage
        df = pd.DataFrame(col.find({},{'_id' : 0}))
        FIXHAI = df[['날짜', 'FIXHAI']]
        MDSP = df[['날짜', 'MDSP']]
        RHORUSQ156N = df[['날짜', 'RHORUSQ156N']]
        result = {
            'FIXHAI' : tools.날짜전처리(FIXHAI),
            'MDSP' : tools.날짜전처리(MDSP),
            'RHORUSQ156N' : tools.날짜전처리(RHORUSQ156N)
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/interestRate')
async def InterestRate():
    try:
        col = client.Fundamental.InterestRate
        df = pd.DataFrame(col.find({},{'_id' : 0}))
        기준금리 = df[['날짜', '기준금리']]
        IORB = df[['날짜', 'IORB']]
        
        result = {
            '기준금리' : tools.날짜전처리(기준금리),
            'IORB' : tools.날짜전처리(IORB)
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/FOMC_clock')
async def FOMC_clock():
    try:
        col = client.Schedule.FOMC_clock
        return list(col.find({}, {'_id':False}))

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