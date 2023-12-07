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
import Api.tools as tools
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])


@router.get("/IndexMA")
async def get_elw_bar_data():
    result = client['AoX']['IndexMA']
    response_data = list(result.find({}, {'_id':False}))
    
    Kospi_MA50, Kospi_MA112, Kospi200_MA50, Kospi200_MA112, Kosdaq_MA50, Kosdaq_MA112 = [], [], [], [], [], []
    
    for value in response_data:
        날짜 = value['날짜'].timestamp() * 1000 
        
        if value['코스피_전체'] != 0:
            Kospi_MA50.append([날짜, (value['코스피_위_MA50'] / value['코스피_전체']) * 100])
            Kospi_MA112.append([날짜, (value['코스피_위_MA50'] / value['코스피_전체']) * 100])
        else:
            Kospi_MA50.append([날짜, 0])  
            Kospi_MA112.append([날짜, 0]) 
        
        if value['코스피200_전체'] != 0:
            Kospi200_MA50.append([날짜, (value['코스피200_위_MA50'] / value['코스피200_전체']) * 100])
            Kospi200_MA112.append([날짜, (value['코스피200_위_MA112'] / value['코스피200_전체']) * 100])
        else:
            Kospi200_MA50.append([날짜, 0]) 
            Kospi200_MA112.append([날짜, 0])
        
        if value['코스닥_전체'] != 0:
            Kosdaq_MA50.append([날짜, (value['코스닥_위_MA50'] / value['코스닥_전체']) * 100])
            Kosdaq_MA112.append([날짜, (value['코스닥_위_MA112'] / value['코스닥_전체']) * 100])    
        else:
            Kosdaq_MA50.append([날짜, 0])  
            Kosdaq_MA112.append([날짜, 0]) 

    result = {
        "Kospi_MA50" : Kospi_MA50,
        "Kospi_MA112" : Kospi_MA112,
        "Kospi200_MA50" : Kospi200_MA50,
        "Kospi200_MA112" : Kospi200_MA112,
        "Kosdaq_MA50" : Kosdaq_MA50,
        "Kosdaq_MA112" : Kosdaq_MA112
    }
    
    return result

# VIX 데이터 처리 함수
def process_vix_data(result):
    Vix = pd.DataFrame(result.find({}, {'_id':0}))
    moving_averages = [2, 3, 4, 5, 6, 9, 10, 12, 15, 18, 20, 25, 27, 36, 45, 60, 112, 224]
    Vix['날짜'] = pd.to_datetime(Vix['날짜']).astype('int64') // 10**6
    for ma in moving_averages:
        Vix[f'MA{ma}'] = Vix['종가'].rolling(window=ma).mean()
    
    response_data = {f'MA{ma}': [] for ma in moving_averages}
    response_data['VIX'] = []

    for value in Vix[-200:].to_dict(orient='records'):
        response_data['VIX'].append([value['날짜'], value['시가'], value['고가'], value['저가'], value['종가']])
        for ma in moving_averages:
            response_data[f'MA{ma}'].append([value['날짜'], value[f'MA{ma}']])
    
    return response_data

@router.get("/VixMA")
async def VixMA( last : str = Query(None), skip: int=0, limit: int=3000 ):
    try :
        result = client['Indices']['Vix']
        
        if last :
            data = list(result.find({}, {'_id':0}).sort([('날짜', -1)]).limit(2))
            return data
        else :
            return process_vix_data(result)
    
    except Exception as e:
        print('VixMA :', e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/{name}')
async def loadDB(name):
    try :
        result = client['Indices'][name]

        if name == 'WorldIndex' :
            return list(result.find({}, {'_id':False}))
        else :
            data = pd.DataFrame(result.find({}, {'_id':0, '거래량' : 0, '거래대금' : 0}))
            # data['날짜'] = pd.to_datetime(data['날짜']).astype('int64') // 10**6
            # data = data.to_dict(orient='split')
            stock = tools.날짜전처리(data)
            # stock = []
            # for item in data.to_dict(orient='records') :
            #     stock.append([item['날짜'], item['시가'],item['고가'],item['저가'],item['종가']])
            return stock
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})


# @router.get('/VixMA')
# async def ElwRatioData():
#     file_path = 'D:/Data/vix.json'
#     with open(file_path, encoding='utf-8') as json_file:
#         data = json.load(json_file)
    
#     VIX, MA2, MA3, MA4, MA5, MA6, MA9, MA10, MA12, MA15, MA18, MA20, MA25, MA27, MA36, MA45, MA60, MA112, MA224 = [], [],[], [],[], [],[], [],[], [],[], [],[], [],[], [],[], [],[]

#     for value in data:
#         # 날짜 = datetime.datetime.fromisoformat(value['날짜']).timestamp() * 1000  # ISO 형식의 날짜를 타임스탬프로 변환
#         VIX.append([value['Date'], value['Open'], value['High'], value['Low'], value['Close']])
#         MA2.append([value['Date'], value['MA2']])
#         MA3.append([value['Date'], value['MA3']])
#         MA4.append([value['Date'], value['MA4']])
#         MA5.append([value['Date'], value['MA5']])
#         MA6.append([value['Date'], value['MA6']])
#         MA9.append([value['Date'], value['MA9']])
#         MA10.append([value['Date'], value['MA10']])
#         MA12.append([value['Date'], value['MA12']])
#         MA15.append([value['Date'], value['MA15']])
#         MA18.append([value['Date'], value['MA18']])
#         MA20.append([value['Date'], value['MA20']])
#         MA25.append([value['Date'], value['MA25']])
#         MA27.append([value['Date'], value['MA27']])
#         MA36.append([value['Date'], value['MA36']])
#         MA45.append([value['Date'], value['MA45']])
#         MA60.append([value['Date'], value['MA60']])
#         MA112.append([value['Date'], value['MA112']])
#         MA224.append([value['Date'], value['MA224']])
        
#     result = {
#         'VIX' : VIX,
#         'MA2' : MA2,
#         'MA3' : MA3,
#         'MA4' : MA4,
#         'MA5' : MA5,
#         'MA6' : MA6,
#         'MA9' : MA9,
#         'MA10' : MA10,
#         'MA12' : MA12,
#         'MA15' : MA15,
#         'MA18' : MA18,
#         'MA20' : MA20,
#         'MA25' : MA25,
#         'MA27' : MA27,
#         'MA36' : MA36,
#         'MA45' : MA45,
#         'MA60' : MA60,
#         'MA112' : MA112,
#         'MA224' : MA224
#     }
    
#     return result

# @router.get('/exNow_KR')
# async def exNow_KR():
#     file_path = 'D:/Data/exNow_KR.json'
#     with open(file_path, encoding='utf-8') as json_file:
#         data = json.load(json_file)
    
#     # 데이터 저장을 위한 리스트 초기화
#     dataArray = [[] for _ in range(12)]
#     지난달_47 = [None] * 12
#     지난달_43 = [None] * 12
#     지난달_41 = [None] * 12
#     지난달_39 = [None] * 12
#     지난달_21 = [None] * 12
#     지난달_10 = [None] * 12
#     지난달_6 = [None] * 12
#     지난달_만기 = [None] * 12
#     지난달_만기월 = [None] * 12

#     # 데이터 처리
#     for item in data:
#         for j in range(12):
#             dataArray[j].append([item['ts'], item[f'지난달{j}_Ref']])

#             if item[f'지난달{j}'] == 47:
#                 지난달_47[j] = item['ts']
#             elif item[f'지난달{j}'] == 43:
#                 지난달_43[j] = item['ts']
#             elif item[f'지난달{j}'] == 41:
#                 지난달_41[j] = item['ts']
#             elif item[f'지난달{j}'] == 39:
#                 지난달_39[j] = item['ts']
#             elif item[f'지난달{j}'] == 21:
#                 지난달_21[j] = item['ts']
#             elif item[f'지난달{j}'] == 10:
#                 지난달_10[j] = item['ts']
#             elif item[f'지난달{j}'] == 6:
#                 지난달_6[j] = item['ts']
#             elif item[f'지난달{j}'] == 1:
#                 지난달_만기[j] = item['ts']
#                 # 만기월 = datetime.fromtimestamp(item['ts'] / 1000).month
#                 # 지난달_만기월[j] = f"{만기월 - 1:02}" if 만기월 > 1 else "12"
#                 지난달_만기월[j] = datetime.fromtimestamp(item['ts'] / 1000).strftime('%m')

#     # 최종 데이터 구조 생성
#     commitData = {}
#     for index in range(12):
#         commitData[f'data{index}'] = dataArray[index]
#         commitData[f'지난달{index}_47'] = 지난달_47[index]
#         commitData[f'지난달{index}_43'] = 지난달_43[index]
#         commitData[f'지난달{index}_41'] = 지난달_41[index]
#         commitData[f'지난달{index}_39'] = 지난달_39[index]
#         commitData[f'지난달{index}_21'] = 지난달_21[index]
#         commitData[f'지난달{index}_10'] = 지난달_10[index]
#         commitData[f'지난달{index}_6'] = 지난달_6[index]
#         commitData[f'지난달{index}_만기'] = 지난달_만기[index]
#         commitData[f'지난달{index}_만기월'] = 지난달_만기월[index]
        
#     return commitData

# @router.get('/exNow_US')
# async def exNow_KR():
    file_path = 'D:/Data/exNow_US.json'
    with open(file_path, encoding='utf-8') as json_file:
        data = json.load(json_file)
    
    # 데이터 저장을 위한 리스트 초기화
    dataArray = [[] for _ in range(13)]
    지난달_47 = [None] * 13
    지난달_43 = [None] * 13
    지난달_41 = [None] * 13
    지난달_39 = [None] * 13
    지난달_21 = [None] * 13
    지난달_10 = [None] * 13
    지난달_5 = [None] * 13
    지난달_만기 = [None] * 13
    지난달_만기월 = [None] * 13

    # 데이터 처리
    for item in data:
        for j in range(12):
            dataArray[j].append([item['ts'], item[f'지난달{j}_Ref']])

            if item[f'지난달{j}'] == 47:
                지난달_47[j] = item['ts']
            elif item[f'지난달{j}'] == 43:
                지난달_43[j] = item['ts']
            elif item[f'지난달{j}'] == 41:
                지난달_41[j] = item['ts']
            elif item[f'지난달{j}'] == 39:
                지난달_39[j] = item['ts']
            elif item[f'지난달{j}'] == 16:
                지난달_21[j] = item['ts']
            elif item[f'지난달{j}'] == 10:
                지난달_10[j] = item['ts']
            elif item[f'지난달{j}'] == 5:
                지난달_5[j] = item['ts']
            elif item[f'지난달{j}'] == 1:
                지난달_만기[j] = item['ts']
                # 만기월 = datetime.fromtimestamp(item['ts'] / 1000).month
                # 지난달_만기월[j] = f"{만기월 - 1:02}" if 만기월 > 1 else "12"
                지난달_만기월[j] = datetime.fromtimestamp(item['ts'] / 1000).strftime('%m')

    # 최종 데이터 구조 생성
    commitData = {}
    DataUS = {}
    for index in range(12):
        DataUS[f'data{index}'] = dataArray[index]
        commitData[f'지난달{index}_47'] = 지난달_47[index]
        commitData[f'지난달{index}_43'] = 지난달_43[index]
        commitData[f'지난달{index}_41'] = 지난달_41[index]
        commitData[f'지난달{index}_39'] = 지난달_39[index]
        commitData[f'지난달{index}_21'] = 지난달_21[index]
        commitData[f'지난달{index}_10'] = 지난달_10[index]
        commitData[f'지난달{index}_6'] = 지난달_5[index]
        commitData[f'지난달{index}_만기'] = 지난달_만기[index]
        commitData[f'지난달{index}_만기월'] = 지난달_만기월[index]
        
    return {"commitData" : commitData, "DataUS" : DataUS}