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

@router.get('/deposit')
async def Deposit():
    try :
        col = client.Indices.Deposit
        df = pd.DataFrame(col.find({},{'_id':0}))
        result = {
            'data' : {
                '고객예탁금' : tools.날짜전처리(df[['날짜', '고객예탁금']]),
                '신용잔고' : tools.날짜전처리(df[['날짜', '신용잔고']]),
            },
            'value' :{
                '금액' : {
                    '고객예탁금' : round(df.iloc[-2:]['고객예탁금'].values[1] / 10000),
                    '신용잔고' : round(df.iloc[-2:]['신용잔고'].values[1]/10000),
                },
                '전일대비' : {
                    '고객예탁금' : round((df.iloc[-2:]['고객예탁금'].values[1] - df.iloc[-2:]['고객예탁금'].values[0]) / 10000),
                    '신용잔고' : round((df.iloc[-2:]['신용잔고'].values[1] - df.iloc[-2:]['신용잔고'].values[0]) / 10000),
                },
            }
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

def getPER_PBR_Data(df):
    result = []
    colors = ['red', 'peru', 'tomato', 'coral', 'wheat', 'orange', 'gold', 'yellow', 'greenyellow', 'pink', 'cyan', 'limegreen', 'deepskyblue', 'dodgerblue', 'mistyrose', 'skyblue', 'beige', 'lightsteelblue', 'lavender', 'plum', 'snow']
    for index, row in df.iterrows():
        # PER 값을 파싱하고, 에러가 발생하면 None으로 설정
        try:
            per = float(row['PER'])
        except ValueError:
            per = None
        item = {
            'name': f"{row['업종명']} ({row['종목수']})",
            'data': [[row['PBR'], per] if per is not None else [row['PBR']]],
            'color': colors[index % len(colors)]
        }
        result.append(item)
    return result

@router.get('/PER_PBR')
async def PER_PBR(name):
    try :
        col = client['Indices'][f'{name}PER_PBR']
        df = pd.DataFrame(col.find({},{'_id' :0}))
        return getPER_PBR_Data(df)
    except Exception as e:
        logging.error(e)
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