from fastapi import APIRouter, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import pymongo
import json
import talib as ta
import pandas as pd
import numpy as np
import logging, ast, requests
from datetime import datetime, timedelta
import Api.tools as tools
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

def 자사주_취득처분(col, code, 취득처분) :
    data = pd.DataFrame(col.find({"종목코드" : code, '취득처분' : 취득처분},{'_id' : 0, '거래일':1}))
    
    result = []
    if len(data) > 0 :
        data['거래일'] = pd.to_datetime(data['거래일']).astype('int64')//10**6    
        datetime = data.to_dict(orient='records')    
        
        color = 'red'
        if 취득처분 == '처분' :
            color = 'dodgerblue'
        for data in datetime :
            add = {
                'color': color, 
                    'width': 2, 
                    'value': data['거래일'], 
                    'label': {
                        'text': f'자사주 {취득처분}',
                        'rotation': 0,
                        'y': -3,
                        'x': 0,
                    }
            }
            result.append(add)
    return result


@router.get('/get/{code}')
async def getStockChartData(code, days=200, week='day') :
    try :
        today = datetime.today()
        endDate = today.strftime('%Y%m%d')
        startDate = today - timedelta(days=days)
        url = f'https://api.finance.naver.com/siseJson.naver?symbol={code}&requestType=1&startTime={startDate}&endTime={endDate}&timeframe={week}'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        with requests.Session() as session:
            response = session.get(url, headers=headers)

            if response.status_code == 200:
                content = response.content.decode('utf-8')
                cleaned_content = content.replace('\n', '').replace('\t', '').replace(' ', '')
                data_list = ast.literal_eval(cleaned_content)

                df = pd.DataFrame(data_list[1:], columns=data_list[0])
                stock = df[['날짜', '시가','고가','저가','종가']]
                volume = df[['날짜', '거래량']]

                col = client.AoX.TreasuryStock
                취득 = 자사주_취득처분(col, code, '취득')
                처분 = 자사주_취득처분(col, code, '처분')
                
                WillR9 = ta.WILLR(stock['고가'], stock['저가'], stock['종가'], timeperiod=9)
                WillR14 = ta.WILLR(stock['고가'], stock['저가'], stock['종가'], timeperiod=14)
                WillR33 = ta.WILLR(stock['고가'], stock['저가'], stock['종가'], timeperiod=33)
                
                willR = {
                    'w9':round(WillR9.to_list()[-1],1),
                    'w14':round(WillR14.to_list()[-1],1),
                    'w33':round(WillR33.to_list()[-1],1)
                }
                
                result = { 'price' : tools.날짜전처리(stock), 'volume' : tools.날짜전처리(volume), 'treasury' : 취득+처분, 'willR' : willR }
                return result

    except Exception as e:
        return {"error" : str(e)}



@router.get('/{code}')
async def loadStock(code, limit: int=1000):
# async def loadStock(code, skip: int=0, limit: int=4000):
    try :
        db = client['Stocks']
        data = db[code]

        df = pd.DataFrame(data.find({}, {'_id':0}).sort([('날짜', -1)]).limit(limit) )
        df['날짜'] = pd.to_datetime(df['날짜'])
        df=df.sort_values(by='날짜')
        stock = df[['날짜', '시가','고가','저가','종가']]
        volume = df[['날짜', '거래량']]

        # df['날짜'] = pd.to_datetime(df['날짜']).astype('int64') // 10**6
        # # mongo_data = list(data.find({}, {'_id':False}).skip(skip).limit(limit))
        # stock, volume = [], []
        # for item in df.to_dict(orient='records') :
        #     stock.append([item['날짜'], item['시가'],item['고가'],item['저가'],item['종가']])
        #     volume.append([item['날짜'], item['거래량']])
        result = { 'stock' : tools.날짜전처리(stock), 'volume' : tools.날짜전처리(volume) }
        return result

        # return mongo_data
    except Exception as e:
        return {"error" : str(e)}
    
@router.get('/{code}/willr')
async def loadStock(code, num:int):
# async def loadStock(code, skip: int=0, limit: int=4000):
    try :
        db = client['Stocks']
        data = db[code]

        df = pd.DataFrame(data.find({}, {'_id':False}))
        df['날짜'] = pd.to_datetime(df['날짜']).astype('int64') // 10**6
        # mongo_data = list(data.find({}, {'_id':False}).skip(skip).limit(limit))
        stock, volume = [], []
        for item in df.to_dict(orient='records') :
            stock.append([item['날짜'], item['시가'],item['고가'],item['저가'],item['종가']])
            volume.append([item['날짜'], item['거래량']])
        result = { 'stock' : stock, 'volume' : volume }
        return result

        # return mongo_data
    except Exception as e:
        return {"error" : str(e)}
    
