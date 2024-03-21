import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pymongo
from pydantic import BaseModel
from fastapi import FastAPI, Request, Response, WebSocket, HTTPException, Depends, APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.websockets import WebSocketDisconnect
from typing import Any
from starlette.middleware.base import BaseHTTPMiddleware
import logging, asyncio, time
import Api.tools as tools

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])


@router.get('/bubbleData')
async def BubbleData():
    try :
        col = client.AoX.BubbleData
        data = pd.DataFrame(col.find({}, {'_id':0}))
        
        조 = 1000000000000
        시총차이 = round((data['시가총액'].sum() - data['전날시가총액'].sum())/조)
        categories, datalist = [], []
        name = ['삼성전자', '2위 ~ 15위', '16위 ~ 50위', '51위 ~ 100위', '101위 ~ 200위']
        for index, row in data.iterrows():
            차이 = round((row['시가총액'] - row['전날시가총액'])/조)
            if 차이 >= 0 :
                categories.append(f"{round(row['시가총액'] / 조)} 조<br>전일대비 <span style='color:#FCAB2F;'> {차이} 조</span>")
            else :
                categories.append(f"{round(row['시가총액'] / 조)} 조<br>전일대비 <span style='color:#00F3FF;'> {차이} 조</span>")
            if row['등락률'] >= 0 :
                color = 'tomato'
            else : color = 'dodgerblue'
            datalist.append({
                'x' : index, 'y' : round(row['등락률'],2), 'z' : row['시가총액']/조, 'name' : name[index], 'color' : color
            })
            
        if data.iloc[0]['전일대비'] >= 0 :
            title = f"코스피200 : {data.iloc[0]['코스피200']} <span style='color:#FCAB2F;'>  +{data.iloc[0]['전일대비']} %</span>    시가총액 : {format(round(data['시가총액'].sum()/조), ',')} 조 ( {시총차이} 조 )"
        else :
            title = f"코스피200 : {data.iloc[0]['코스피200']} <span style='color:#00F3FF;'>  {data.iloc[0]['전일대비']} %</span>    시가총액 : {format(round(data['시가총액'].sum()/조), ',')} 조 ( {시총차이} 조 )"
            
        result = {
            'name' : title,
            'data' : datalist,
            'categories' : categories
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

def create_series(data, name, color, zIndex, yAxis=0, lineType='normal'):
    series = {
        'data': data,
        'name': f'<span style="color : {color};">{name}</span>',
        'color': color,
        'zIndex': zIndex,
        'lineWidth': 1,
        'marker': {'radius': 2.5},
        'yAxis': yAxis
    }
    if lineType != 'normal':
        series.update({
            'type': 'line',
            'dashStyle': 'ShortDash',
            'visible': False if lineType == 'hidden' else 'true'
        })
    return series

async def GroupData(data, name):
    Kospi200, Kospi, Kosdaq, 그룹1, 그룹2, 그룹3, 그룹4, 그룹5, day = [],[],[],[],[],[],[],[],[]
    # 데이터 처리
    if name == 'GroupDataLine' :
        df = data[-10:]
    else : df = data
    
    for item in df :
        Kospi200.append( round(item['코스피200']*100,1) )
        Kospi.append( round(item['코스피']*100,1) )
        Kosdaq.append( round(item['코스닥']*100,1) )
        그룹1.append( round(item['그룹1'],1) )
        그룹2.append( round(item['그룹2'],1) )
        그룹3.append( round(item['그룹3'],1) )
        그룹4.append( round(item['그룹4'],1) )
        그룹5.append( round(item['그룹5'],1) )
        day.append( f"{item['날짜'][4:6]}.{item['날짜'][6:8]}.")

    # 시리즈 생성
    series1 = [create_series(그룹1, '삼성전자', 'tomato', 5),
            create_series(그룹2, '2위 ~ 15위', '#FCAB2F', 4),
            create_series(Kospi, '코스피', 'magenta', 1, 1, 'line'),
            create_series(Kosdaq, '코스닥', 'greenyellow', 1, 1, 'line')]

    series2 = [create_series(그룹3, '16위 ~ 50위', 'greenyellow', 3),
            create_series(그룹4, '51위 ~ 100위', 'dodgerblue', 2),
            create_series(그룹5, '101위 ~ 200위', '#62FFF6', 1),
            create_series(Kospi, '코스피', 'magenta', 1, 1, 'hidden'),
            create_series(Kospi200, '코스피200', '#efe9e9ed', 1, 1, 'line')]
    result = {
        'series1' : series1,
        'series2' : series2,
        'categories' : day
    }
    return result


@router.get('/groupData')
async def GroupDataLine(dbName):
    try :
        col = client.AoX[dbName]
        data = list(col.find({}, {'_id':0}))
        
        return GroupData(data, dbName)
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/marketDaily')
async def MarketDaily():
    try :
        col = client.AoX.MarketDaily
        data = list(col.find({}, {'_id':0}))
        
        Kospi200, Kospi, Kosdaq = [],[],[]
        cate = ['B-5', 'B-4', 'B-3', 'B-2', 'B-1', '09:02', '09:05', '09:07', '09:10', '09:15', '09:20', '09:25', '09:30', '09:35', '09:40', '09:45', '09:50', '09:55', '10:00', '10:05', '10:10', '10:15', '10:20', '10:25', '10:30', '10:35', '10:40', '10:45', '10:50', '10:55', '11:00', '11:05', '11:10', '11:15', '11:20', '11:25', '11:30', '11:35', '11:40', '11:45', '11:50', '11:55', '12:00', '12:05', '12:10', '12:15', '12:20', '12:25', '12:30', '12:35', '12:40', '12:45', '12:50', '12:55', '13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35', '13:40', '13:45', '13:50', '13:55', '14:00', '14:05', '14:10', '14:15', '14:20', '14:25', '14:30', '14:35', '14:40', '14:45', '14:50', '14:55', '15:00', '15:05', '15:10', '15:15', '15:20', '15:25', '15:30']

        for item in data :
            Kospi200.append( round(item['Kospi200'],3) )
            Kospi.append( round(item['Kospi'],3) )
            Kosdaq.append( round(item['Kosdaq'],3) )
        series = [
            {
                'zIndex': 3, 'name': "Kospi200", 'color': "#efe9e9ed", 'data': Kospi200,
            }, {
                'zIndex': 2, 'name': "Kospi", 'color': "magenta", 'data': Kospi,
            }, {
                'zIndex': 1, 'name': "Kosdaq", 'color': "greenyellow", 'data': Kosdaq,
            }
        ]
        result ={
            'series' : series,
            'categories' : cate
        }
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

def trend_series(name, data, color, yAxis=0, pointWidth=None, lineWidth=None, type=None ) :
    series = {
        'name' : name,
        'data' : data,
        'color' : color,
        'yAxis' : yAxis,
    }
    if pointWidth :
        series.update({
            'pointWidth' : pointWidth,
        })
    if type :
        series.update({
            'lineWidh' : lineWidth,
            'type' : type
        })
    return series
@router.get('/trendData')
async def TrendData(limit:int= 35):
    try :
        col = client.AoX.TrendData
        data = pd.DataFrame(col.find({}, {'_id':0}).sort('날짜', -1).limit(limit) )
        data = data.sort_values(by='날짜')

        외국인 = data['외국인_코스피200'].to_list()
        기관 = data['기관_코스피200'].to_list()
        선물 = data['외국인_선물'].to_list()
        콜옵션 = data['외국인_콜옵션'].to_list()
        풋옵션 = data['외국인_풋옵션'].to_list()

        현물 = (data['외국인_코스피200'] + data['기관_코스피200']).to_list()
        옵션 = (data['외국인_콜옵션'] + data['외국인_풋옵션']).to_list()

        series = [
            trend_series(name = '외국인', data=외국인, color = '#FCAB2F', pointWidth=8 ),
            trend_series(name = '개관', data=기관, color = 'forestgreen', pointWidth=4 ),
            trend_series(name = '선물', data=선물, color = 'magenta', type='spline', lineWidth=0.5, yAxis=2 ),
            trend_series(name = '콜옵션', data=콜옵션, color = 'tomato', type='spline', lineWidth=0.5, yAxis=1 ),
            trend_series(name = '풋옵션', data=풋옵션, color = 'dodgerblue', type='spline', lineWidth=0.5, yAxis=1 )
        ]
        카테고리 = []
        for _, row in data.iterrows():
            카테고리.append(row['날짜'].strftime('%m.%d.'))
        tmp = data[-1:].to_dict(orient='records')[0]
        foreigner = {
            '당일' : [tmp['외국인_코스피200'], tmp['외국인_코스피'], tmp['외국인_코스닥'], tmp['외국인_선물'], tmp['외국인_콜옵션'], tmp['외국인_풋옵션']],
            '누적' : [tmp['외국인_코스피200_누적'], tmp['외국인_코스피_누적'], tmp['외국인_코스닥_누적'], tmp['외국인_선물_누적'], tmp['외국인_콜옵션_누적'], tmp['외국인_풋옵션_누적']]
        }
        institutional ={
            '당일': [tmp['기관_코스피200'], tmp['기관_코스피'], tmp['기관_코스닥'], tmp['기관_선물'], tmp['기관_콜옵션'], tmp['기관_풋옵션']],
            '누적': [tmp['기관_코스피200_누적'], tmp['기관_코스피_누적'], tmp['기관_코스닥_누적'], tmp['기관_선물_누적'], tmp['기관_콜옵션_누적'], tmp['기관_풋옵션_누적']],
        }
        individual = {
            '당일': [tmp['개인_코스피200'], tmp['개인_코스피'], tmp['개인_코스닥'], tmp['개인_선물'], tmp['개인_콜옵션'], tmp['개인_풋옵션']],
            '누적': [tmp['개인_코스피200_누적'], tmp['개인_코스피_누적'], tmp['개인_코스닥_누적'], tmp['개인_선물_누적'], tmp['개인_콜옵션_누적'], tmp['개인_풋옵션_누적']],
        }

        table2 = [{ '구분': '코스피200', '외국인': tmp['외국인_코스피200'], '외국인_누적': tmp['외국인_코스피200_누적'], '기관': tmp['기관_코스피200'], '기관_누적': tmp['기관_코스피200_누적'], '개인': tmp['개인_코스피200'], '개인_누적': tmp['개인_코스피200_누적'] },
                { '구분': '코스피', '외국인': tmp['외국인_코스피'], '외국인_누적': tmp['외국인_코스피_누적'], '기관': tmp['기관_코스피'], '기관_누적': tmp['기관_코스피_누적'], '개인': tmp['개인_코스피'], '개인_누적': tmp['개인_코스피_누적'] },
                { '구분': '코스닥', '외국인': tmp['외국인_코스닥'], '외국인_누적': tmp['외국인_코스닥_누적'], '기관': tmp['기관_코스닥'], '기관_누적': tmp['기관_코스닥_누적'], '개인': tmp['개인_코스닥'], '개인_누적': tmp['개인_코스닥_누적'] },
                { '구분': '선물', '외국인': tmp['외국인_선물'], '외국인_누적': tmp['외국인_선물_누적'], '기관': tmp['기관_선물'], '기관_누적': tmp['기관_선물_누적'], '개인': tmp['개인_선물'], '개인_누적': tmp['개인_선물_누적'] },
                { '구분': '콜옵션', '외국인': tmp['외국인_콜옵션'], '외국인_누적': tmp['외국인_콜옵션_누적'], '기관': tmp['기관_콜옵션'], '기관_누적': tmp['기관_콜옵션_누적'], '개인': tmp['개인_콜옵션'], '개인_누적': tmp['개인_콜옵션_누적'] },
                { '구분': '풋옵션', '외국인': tmp['외국인_풋옵션'], '외국인_누적': tmp['외국인_풋옵션_누적'], '기관': tmp['기관_풋옵션'], '기관_누적': tmp['기관_풋옵션_누적'], '개인': tmp['개인_풋옵션'], '개인_누적': tmp['개인_풋옵션_누적'] }]
        result = {
            'series' : series,
            'categories' : 카테고리,
            'yAxis0Abs' : max(abs(x) for x in 현물),
            'yAxis1Abs' : max(abs(x) for x in 옵션),
            'yAxis2Abs' : max(abs(x) for x in 선물),
            'foreigner' : foreigner,
            'institutional' : institutional,
            'individual' : individual,
            'table2' : table2,
            }

        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})


async def Gpo_Kospi200(req : Request):
    while True:
        is_disconnected = await req.is_disconnected()
        if is_disconnected: break
        
        col = client.GPO.StartDate
        res = list(col.find({},{'_id':0}))
        date = res[0]['날짜']
        col = client.Indices.Kospi200
        data = pd.DataFrame(col.find({ '날짜' : {'$gte' : date }  }, {'_id':0, '거래량':0, '거래대금':0}))
        
        df = pd.merge(pd.DataFrame(res[1]), data, how='left').fillna(0)
        df['날짜'] = pd.to_datetime(df['날짜']).astype('int64') // 10**6
                
        response_data = {
            'data': df.to_dict(orient='split')['data'],
            'min': data['저가'].min()
        }
        
        json_data = json.dumps(response_data)
        yield f"data: {json_data}\n\n"
        
        if tools.장중확인 == False : break
        await asyncio.sleep(120)
        
@router.get('/gpo')
async def Gpo(req : Request):
    return StreamingResponse(Gpo_Kospi200(req), media_type='text/event-stream')


# MainPage API
async def create_trend_data(limit:int= 35):
    """ 외국인, 기관, 개인 투자자 매매동향 
    table, Bar Chart Data

    Args:
        limit (int, optional): _description_. Defaults to 35.

    Returns:
        _type_: _description_
    """
    col = client.AoX.TrendData
    data = pd.DataFrame(col.find({}, {'_id':0}).sort('날짜', -1).limit(limit) )
    data = data.sort_values(by='날짜')

    외국인 = data['외국인_코스피200'].to_list()
    기관 = data['기관_코스피200'].to_list()
    선물 = data['외국인_선물'].to_list()
    콜옵션 = data['외국인_콜옵션'].to_list()
    풋옵션 = data['외국인_풋옵션'].to_list()

    현물 = (data['외국인_코스피200'] + data['기관_코스피200']).to_list()
    옵션 = (data['외국인_콜옵션'] + data['외국인_풋옵션']).to_list()

    series = [
        trend_series(name = '외국인', data=외국인, color = '#FCAB2F', pointWidth=8 ),
        trend_series(name = '개관', data=기관, color = 'forestgreen', pointWidth=4 ),
        trend_series(name = '선물', data=선물, color = 'magenta', type='spline', lineWidth=0.5, yAxis=2 ),
        trend_series(name = '콜옵션', data=콜옵션, color = 'tomato', type='spline', lineWidth=0.5, yAxis=1 ),
        trend_series(name = '풋옵션', data=풋옵션, color = 'dodgerblue', type='spline', lineWidth=0.5, yAxis=1 )
    ]
    카테고리 = []
    for _, row in data.iterrows():
        카테고리.append(row['날짜'].strftime('%m.%d.'))
    tmp = data[-1:].to_dict(orient='records')[0]
    foreigner = {
        '당일' : [tmp['외국인_코스피200'], tmp['외국인_코스피'], tmp['외국인_코스닥'], tmp['외국인_선물'], tmp['외국인_콜옵션'], tmp['외국인_풋옵션']],
        '누적' : [tmp['외국인_코스피200_누적'], tmp['외국인_코스피_누적'], tmp['외국인_코스닥_누적'], tmp['외국인_선물_누적'], tmp['외국인_콜옵션_누적'], tmp['외국인_풋옵션_누적']]
    }
    institutional ={
        '당일': [tmp['기관_코스피200'], tmp['기관_코스피'], tmp['기관_코스닥'], tmp['기관_선물'], tmp['기관_콜옵션'], tmp['기관_풋옵션']],
        '누적': [tmp['기관_코스피200_누적'], tmp['기관_코스피_누적'], tmp['기관_코스닥_누적'], tmp['기관_선물_누적'], tmp['기관_콜옵션_누적'], tmp['기관_풋옵션_누적']],
    }
    individual = {
        '당일': [tmp['개인_코스피200'], tmp['개인_코스피'], tmp['개인_코스닥'], tmp['개인_선물'], tmp['개인_콜옵션'], tmp['개인_풋옵션']],
        '누적': [tmp['개인_코스피200_누적'], tmp['개인_코스피_누적'], tmp['개인_코스닥_누적'], tmp['개인_선물_누적'], tmp['개인_콜옵션_누적'], tmp['개인_풋옵션_누적']],
    }

    table2 = [{ '구분': '코스피200', '외국인': tmp['외국인_코스피200'], '외국인_누적': tmp['외국인_코스피200_누적'], '기관': tmp['기관_코스피200'], '기관_누적': tmp['기관_코스피200_누적'], '개인': tmp['개인_코스피200'], '개인_누적': tmp['개인_코스피200_누적'] },
            { '구분': '코스피', '외국인': tmp['외국인_코스피'], '외국인_누적': tmp['외국인_코스피_누적'], '기관': tmp['기관_코스피'], '기관_누적': tmp['기관_코스피_누적'], '개인': tmp['개인_코스피'], '개인_누적': tmp['개인_코스피_누적'] },
            { '구분': '코스닥', '외국인': tmp['외국인_코스닥'], '외국인_누적': tmp['외국인_코스닥_누적'], '기관': tmp['기관_코스닥'], '기관_누적': tmp['기관_코스닥_누적'], '개인': tmp['개인_코스닥'], '개인_누적': tmp['개인_코스닥_누적'] },
            { '구분': '선물', '외국인': tmp['외국인_선물'], '외국인_누적': tmp['외국인_선물_누적'], '기관': tmp['기관_선물'], '기관_누적': tmp['기관_선물_누적'], '개인': tmp['개인_선물'], '개인_누적': tmp['개인_선물_누적'] },
            { '구분': '콜옵션', '외국인': tmp['외국인_콜옵션'], '외국인_누적': tmp['외국인_콜옵션_누적'], '기관': tmp['기관_콜옵션'], '기관_누적': tmp['기관_콜옵션_누적'], '개인': tmp['개인_콜옵션'], '개인_누적': tmp['개인_콜옵션_누적'] },
            { '구분': '풋옵션', '외국인': tmp['외국인_풋옵션'], '외국인_누적': tmp['외국인_풋옵션_누적'], '기관': tmp['기관_풋옵션'], '기관_누적': tmp['기관_풋옵션_누적'], '개인': tmp['개인_풋옵션'], '개인_누적': tmp['개인_풋옵션_누적'] }]
    result = {
        'series' : series,
        'categories' : 카테고리,
        'yAxis0Abs' : max(abs(x) for x in 현물),
        'yAxis1Abs' : max(abs(x) for x in 옵션),
        'yAxis2Abs' : max(abs(x) for x in 선물),
        'foreigner' : foreigner,
        'institutional' : institutional,
        'individual' : individual,
        'table2' : table2,
        }

    return result

async def create_market_daily():
    col = client.AoX.MarketDaily
    data = list(col.find({}, {'_id':0}))
    
    Kospi200, Kospi, Kosdaq = [],[],[]
    cate = ['B-5', 'B-4', 'B-3', 'B-2', 'B-1', '09:02', '09:05', '09:07', '09:10', '09:15', '09:20', '09:25', '09:30', '09:35', '09:40', '09:45', '09:50', '09:55', '10:00', '10:05', '10:10', '10:15', '10:20', '10:25', '10:30', '10:35', '10:40', '10:45', '10:50', '10:55', '11:00', '11:05', '11:10', '11:15', '11:20', '11:25', '11:30', '11:35', '11:40', '11:45', '11:50', '11:55', '12:00', '12:05', '12:10', '12:15', '12:20', '12:25', '12:30', '12:35', '12:40', '12:45', '12:50', '12:55', '13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35', '13:40', '13:45', '13:50', '13:55', '14:00', '14:05', '14:10', '14:15', '14:20', '14:25', '14:30', '14:35', '14:40', '14:45', '14:50', '14:55', '15:00', '15:05', '15:10', '15:15', '15:20', '15:25', '15:30']

    for item in data :
        Kospi200.append( round(item['Kospi200'],3) )
        Kospi.append( round(item['Kospi'],3) )
        Kosdaq.append( round(item['Kosdaq'],3) )
    series = [
        {
            'zIndex': 3, 'name': "Kospi200", 'color': "#efe9e9ed", 'data': Kospi200,
        }, {
            'zIndex': 2, 'name': "Kospi", 'color': "magenta", 'data': Kospi,
        }, {
            'zIndex': 1, 'name': "Kosdaq", 'color': "greenyellow", 'data': Kosdaq,
        }
    ]
    result ={
        'series' : series,
        'categories' : cate
    }
    return result

async def create_bubble_data():
    
    col = client.AoX.BubbleData
    data = pd.DataFrame(col.find({}, {'_id':0}))
    
    조 = 1000000000000
    시총차이 = round((data['시가총액'].sum() - data['전날시가총액'].sum())/조)
    categories, datalist = [], []
    name = ['삼성전자', '2위 ~ 15위', '16위 ~ 50위', '51위 ~ 100위', '101위 ~ 200위']
    for index, row in data.iterrows():
        차이 = round((row['시가총액'] - row['전날시가총액'])/조)
        if 차이 >= 0 :
            categories.append(f"{round(row['시가총액'] / 조)} 조<br>전일대비 <span style='color:#FCAB2F;'> {차이} 조</span>")
        else :
            categories.append(f"{round(row['시가총액'] / 조)} 조<br>전일대비 <span style='color:#00F3FF;'> {차이} 조</span>")
        if row['등락률'] >= 0 :
            color = 'tomato'
        else : color = 'dodgerblue'
        datalist.append({
            'x' : index, 'y' : round(row['등락률'],2), 'z' : row['시가총액']/조, 'name' : name[index], 'color' : color
        })
        
    if data.iloc[0]['전일대비'] >= 0 :
        title = f"코스피200 : {data.iloc[0]['코스피200']} <span style='color:#FCAB2F;'>  +{data.iloc[0]['전일대비']} %</span>    시가총액 : {format(round(data['시가총액'].sum()/조), ',')} 조 ( {시총차이} 조 )"
    else :
        title = f"코스피200 : {data.iloc[0]['코스피200']} <span style='color:#00F3FF;'>  {data.iloc[0]['전일대비']} %</span>    시가총액 : {format(round(data['시가총액'].sum()/조), ',')} 조 ( {시총차이} 조 )"
    
    result = {
        'series': [{
            'name': title,
            'data': datalist,
            'animation': False,
        }],
        'categories': categories}
    
    return result

async def create_group_data():
    col = client.AoX.GroupDataLine
    data = list(col.find({}, {'_id':0}))
    
    return await GroupData(data, 'GroupDataLine')

async def get_main_data(req:Request):
    while True:
        is_disconnected = await req.is_disconnected()
        if is_disconnected: break
        
        result = {
            'TrendData' : await create_trend_data(),
            'MarketDaily' : await create_market_daily(),
            'BubbleData' : await create_bubble_data(),
            'GroupData' : await create_group_data()
        }
        
        json_data = json.dumps(result)
        yield f"data: {json_data}\n\n"
        await asyncio.sleep(120)

@router.get('/main')
async def Stream_main(req : Request):
    response_stream = get_main_data(req)
    return StreamingResponse(response_stream, media_type='text/event-stream')





@router.get('/{name}')
async def loadDB(name):
    try :
        col = client['AoX'][name]
        return list(col.find({}, {'_id':False}))
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
