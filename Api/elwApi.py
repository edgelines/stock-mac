from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
import pymongo, json, logging, asyncio
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

# @router.get("/CTP")
# async def get_elw_bar_data():
#     # result = client['AoX']['elwBarData']
#     result = client['ELW']['ElwBarData']
#     data = pd.DataFrame(result.find({}, {'_id':False}))
#     # 월별로 데이터를 필터링합니다.
#     data_by_month = [data[data['월구분'] == str(month)] for month in range(1, 7)]

#     # 각 월별 데이터에 대한 처리
#     result = []
#     for month_data in data_by_month:
#         콜_5일평균거래대금 = month_data['콜_5일평균거래대금'].to_list()
#         콜_거래대금 = month_data['콜_거래대금'].to_list()
#         풋_5일평균거래대금 = month_data['풋_5일평균거래대금'].to_list()
#         풋_거래대금 = month_data['풋_거래대금'].to_list()
#         행사가 = month_data['행사가'].to_list()
        
#         call_sum = abs(month_data['콜_거래대금'].sum())
#         put_sum = abs(month_data['풋_거래대금'].sum())

#         title = month_data['잔존만기'].values[0]

#         # 분모가 0인지 확인하고 비율 계산
#         if call_sum + put_sum == 0:
#             콜비율 = 0  # 또는 0.0 또는 적절한 다른 값
#             풋비율 = 0  # 또는 0.0 또는 적절한 다른 값
#         else:
#             콜비율 = f'{call_sum / (call_sum + put_sum):.2f}'
#             풋비율 = f'{put_sum / (call_sum + put_sum):.2f}'

#         비율 = f' [ C : <span style="color:greenyellow;"> {콜비율} </span>, P : <span style="color:greenyellow;"> {풋비율} </span> ]'
#         콜범주 = f'Call ( <span style="color:greenyellow;"> {int(call_sum / 100000000)} </span> 억 )'
#         풋범주 = f'Put ( <span style="color:greenyellow;"> {int(put_sum / 100000000)} </span>  억 )'
#         # 결과를 저장합니다.
#         result.append({
#             "title": title, 
#             "콜5일": 콜_5일평균거래대금, 
#             "콜": 콜_거래대금, 
#             "풋5일": 풋_5일평균거래대금, 
#             "풋": 풋_거래대금, 
#             "행사가": 행사가, 
#             "비율": 비율, 
#             "콜범주": 콜범주, 
#             "풋범주": 풋범주, 
#             "콜비율": 콜비율, 
#             "풋비율": 풋비율 
#         })
#     for i in range(len(result)):
#         for key in result[i]:
#             if isinstance(result[i][key], np.int64):
#                 result[i][key] = int(result[i][key])  # np.int64를 int로 변환
#     encoded_result = jsonable_encoder(result)

#     return encoded_result

# # CallPutPage 왼쪽 상단 Chart
# @router.get('/ElwPutCallRatioData')
# async def ElwPutCallRatioData():
#     # result = client['AoX']['ElwPutCallRatioData']
#     result = client['ELW']['ElwPutCallRatioData']
#     data = list(result.find({}, {'_id':0}))
    
#     Day1, Day2, Day3, Day4, Day5, Day20, Day100 = [], [], [], [], [], [], []

#     for value in data:
#         Day1.append([value['날짜'], value['풋_거래대금'] / value['콜_거래대금']])
#         if value['비율_2일'] != '' : 
#             Day2.append([value['날짜'], value['비율_2일']])
#         if value['비율_3일'] != '' : 
#             Day3.append([value['날짜'], value['비율_3일']])
#         if value['비율_4일'] != '' : 
#             Day4.append([value['날짜'], value['비율_4일']])
#         if value['비율_5일'] != '' : 
#             Day5.append([value['날짜'], value['비율_5일']])
#         if value['비율_20일'] != '' : 
#             Day20.append([value['날짜'], value['비율_20일']])
#         if value['비율_100일'] != '' : 
#             Day100.append([value['날짜'], value['비율_100일']])
    
#     result = {
#             "Day1": Day1, 
#             "Day2": Day2, 
#             "Day3": Day3, 
#             "Day4": Day4, 
#             "Day5": Day5, 
#             "Day20": Day20, 
#             "Day100": Day100
            
#         }
    
#     return result

# CallPutPage 가운데 상단 Chart
@router.get('/DayGr')
async def DayGr():
    result = client['ELW']['DayGr']
    response_data = list(result.find({}, {'_id':False}))
    
    data1, data2, data3, data4, Day = [], [], [], [], []

    for index, value in enumerate(response_data):
        if value['권리유형'] == '콜1':
            data1.append(int(value['거래대금']))
        elif value['권리유형'] == '콜2':
            data2.append(int(value['거래대금']))
        elif value['권리유형'] == '풋1':
            data3.append(int(value['거래대금']))
        elif value['권리유형'] == '풋2':
            data4.append(int(value['거래대금']))

        if index % 4 == 0:
            next_value = response_data[index + 1] if index + 1 < len(response_data) else None
            day_string = value['날짜'][4:6] + '.' + value['날짜'][6:8] + '.<br>' + str(value['영업일']) + '일<br>'
            if next_value:
                day_string += str(next_value['영업일']) + '일'
            Day.append(day_string)

    # 배열의 마지막 요소들을 사용하여 문자열 생성
    잔존만기1 = str(response_data[-2]['잔존만기'])
    잔존만기2 = str(response_data[-1]['잔존만기'])

    callName1 = f" (<span style='color:greenyellow;'>{str(int(response_data[-4]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {response_data[-2]['영업일']} ]"
    callName2 = f" (<span style='color:greenyellow;'>{str(int(response_data[-3]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {response_data[-1]['영업일']} ]"
    putName1 = f" (<span style='color:greenyellow;'>{str(int(response_data[-2]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {response_data[-2]['영업일']} ]"
    putName2 = f" (<span style='color:greenyellow;'>{str(int(response_data[-1]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {response_data[-1]['영업일']} ]"
    
    result = {
            "call1": 잔존만기1 + callName1,
            "put1": 잔존만기1 + putName1,
            "call2": 잔존만기2 + callName2,
            "put2": 잔존만기2 + putName2,
            "data1": data1, 
            "data2": data2, 
            "data3": data3,
            "data4": data4,
            "Day": Day,
        }
    
    return result

# CallPutPage 오른쪽 상단 Chart
@router.get('/ElwRatioData')
async def ElwRatioData():
    result = client['ELW']['ElwRatioData']
    data = pd.DataFrame(result.find({}, {'_id':0}).sort([('날짜', -1)]).limit(8))
    data=data.sort_values(by='날짜')
    data = data.to_dict(orient='records')
    call, put, category = [],[],[]

    for value in data:
        콜비율 = round(value['콜_거래대금'] / (value['콜_거래대금'] + value['풋_거래대금']) * 100, 2)
        풋비율 = round(value['풋_거래대금'] / (value['콜_거래대금'] + value['풋_거래대금']) * 100, 2)

        날짜문자열 = str(value['날짜'])
        날짜포맷 = 날짜문자열[4:6] + '.' + 날짜문자열[6:8]
        카테고리문자열 = f"{날짜포맷}<br><span style=\"color:#FCAB2F\">{콜비율} %</span><br><span style=\"color:#00F3FF\">{풋비율} %</span>"

        call.append(콜비율)
        put.append(풋비율)
        category.append(카테고리문자열)
    
    result = {
            "call": call, 
            "put": put, 
            "category": category
        }
    
    return result


# CTP 3개 
async def get_elw_bar_data(req: Request):
    while True:
        is_disconnected = await req.is_disconnected()
        if is_disconnected: break
        
        result = client['ELW']['ElwBarData']
        data = pd.DataFrame(result.find({}, {'_id':False}))
        # 월별로 데이터를 필터링합니다.
        data_by_month = [data[data['월구분'] == str(month)] for month in range(1, 4)]

        # 각 월별 데이터에 대한 처리
        result = []
        for month_data in data_by_month:
            콜_5일평균거래대금 = month_data['콜_5일평균거래대금'].to_list()
            콜_거래대금 = month_data['콜_거래대금'].to_list()
            풋_5일평균거래대금 = month_data['풋_5일평균거래대금'].to_list()
            풋_거래대금 = month_data['풋_거래대금'].to_list()
            행사가 = month_data['행사가'].to_list()
            
            call_sum = abs(month_data['콜_거래대금'].sum())
            put_sum = abs(month_data['풋_거래대금'].sum())

            title = month_data['잔존만기'].values[0]

            # 분모가 0인지 확인하고 비율 계산
            if call_sum + put_sum == 0:
                콜비율 = 0  # 또는 0.0 또는 적절한 다른 값
                풋비율 = 0  # 또는 0.0 또는 적절한 다른 값
            else:
                콜비율 = f'{call_sum / (call_sum + put_sum):.2f}'
                풋비율 = f'{put_sum / (call_sum + put_sum):.2f}'

            비율 = f' [ C : <span style="color:greenyellow;"> {콜비율} </span>, P : <span style="color:greenyellow;"> {풋비율} </span> ]'
            콜범주 = f'Call ( <span style="color:greenyellow;"> {int(call_sum / 100000000)} </span> 억 )'
            풋범주 = f'Put ( <span style="color:greenyellow;"> {int(put_sum / 100000000)} </span>  억 )'
            # 결과를 저장합니다.
            result.append({
                "title": title, 
                "콜5일": 콜_5일평균거래대금, 
                "콜": 콜_거래대금, 
                "풋5일": 풋_5일평균거래대금, 
                "풋": 풋_거래대금, 
                "행사가": 행사가, 
                "비율": 비율, 
                "콜범주": 콜범주, 
                "풋범주": 풋범주, 
                "콜비율": 콜비율, 
                "풋비율": 풋비율 
            })
        for i in range(len(result)):
            for key in result[i]:
                if isinstance(result[i][key], np.int64):
                    result[i][key] = int(result[i][key])  # np.int64를 int로 변환
        encoded_result = jsonable_encoder(result)

        json_data = json.dumps(encoded_result)
        yield f"data: {json_data}\n\n"
        await asyncio.sleep(120)
        
    
@router.get('/CTP')
async def Stream_CTP(req : Request):
    return StreamingResponse(get_elw_bar_data(req), media_type='text/event-stream')

# CTP > C, X, 1/2, P Box Table
async def get_weighted_avg(req:Request):
    while True:
        is_disconnected = await req.is_disconnected()
        if is_disconnected: break
        
        col = client['ELW']['WeightedAvg']
        data = pd.DataFrame(col.find({},{'_id':0}))
        data = data.fillna(0)
        
        json_data = json.dumps(data[:3].to_dict(orient='records'))
        yield f"data: {json_data}\n\n"
        await asyncio.sleep(120)

@router.get('/WeightedAvg')
async def Stream_CTP(req : Request):
    return StreamingResponse(get_weighted_avg(req), media_type='text/event-stream')




#WA Page API
async def create_WA_data(data:list):
    call, put, kospi200, call_mean, put_mean, mean1, mean2, CTP1, CTP15, CTP2, min_values, dates = [], [], [], [], [], [], [], [], [], [], [], []

    for value in data[-11:] :
        call.append([value['콜_최소'], value['콜_최대']])
        put.append([value['풋_최소'], value['풋_최대']])
        kospi200.append(value['종가'])
        call_mean.append( round(value['콜_가중평균'],1 ) )
        put_mean.append( round(value['풋_가중평균'],1 ) )
        mean1.append( round(((value['콜_가중평균'] + value['풋_가중평균']) / 2),1 ) )
        mean2.append( round(value['콜풋_가중평균'],1 ) )
        CTP1.append( round(value['1일'],1 ) )
        CTP15.append( round(value['1_5일'],1 ) )
        CTP2.append( round(value['2일'],1 ) )
        
        if value['콜_최소'] > 1:
            min_values.append(value['콜_최소'])
        if value['풋_최소'] > 1:
            min_values.append(value['풋_최소'])

        dates.append(value['최종거래일'][2:4] + '.' + value['최종거래일'][5:7] + '.' + value['최종거래일'][8:10] + '.')

        min1 = min(min_values)

    result = {
        'series': [
            {'name': 'Call', 'type': 'errorbar', 'color': '#FCAB2F', 'lineWidth': 2, 'data': call},
            {'name': "Put", 'type': "errorbar", 'color': "#00F3FF", 'lineWidth': 2, 'data': put },
            {'name': "Kospi200", 'type': "spline", 'color': "#efe9e9ed", 'data': kospi200, 'marker': { 'radius': 5 }, 'lineWidth': 1.5, 'zIndex': 5, },
            {'name': "Call_mean", 'type': "spline", 'color': "#FCAB2F", 'data': call_mean, 'lineWidth': 1, 'marker': { 'symbol': "diamond", 'radius': 5 }, },
            {'name': "Put_mean", 'type': "spline", 'color': "#00F3FF", 'data': put_mean, 'lineWidth': 0, 'marker': { 'symbol': "diamond", 'radius': 5 }, },
            {'name': "1/2 (단순)", 'type': "line", 'color': "tomato", 'data': mean1, 'lineWidth': 1, 'marker': { 'symbol': "cross", 'radius': 8, 'lineColor': None, 'lineWidth': 2 }, },
            {'name': "가중", 'type': "line", 'color': "greenyellow", 'data': mean2, 'lineWidth': 1, 'marker': { 'symbol': "cross", 'radius': 8, 'lineColor': None, 'lineWidth': 2 }, },
            {'name': "1일", 'type': "spline", 'color': "pink", 'data': CTP1, 'lineWidth': 1, 'marker': { 'symbol': "circle", 'radius': 3 }, },
            {'name': "1.5일", 'type': "spline", 'color': "gold", 'data': CTP15, 'lineWidth': 0, 'marker': { 'symbol': "circle", 'radius': 3 }, },
            {'name': "2일", 'type': "spline", 'color': "magenta", 'data': CTP2, 'lineWidth': 0, 'marker': { 'symbol': "circle", 'radius': 3 }, },
        ],
        'min': min1,
        'categories': dates,
        'CTP' : CTP1
    }
    
    return result

async def create_DayGr_data():
    col = client.ELW.DayGr
    data_ = list(col.find({},{'_id':0}))
    data1, data2, data3, data4, Day = [], [], [], [], []

    for index, value in enumerate(data_):
        if value['권리유형'] == '콜1':
            data1.append(int(value['거래대금']))
        elif value['권리유형'] == '콜2':
            data2.append(int(value['거래대금']))
        elif value['권리유형'] == '풋1':
            data3.append(int(value['거래대금']))
        elif value['권리유형'] == '풋2':
            data4.append(int(value['거래대금']))

        if index % 4 == 0:
            next_value = data_[index + 1] if index + 1 < len(data_) else None
            day_string = value['날짜'][4:6] + '.' + value['날짜'][6:8] + '.<br>' + str(value['영업일']) + '일<br>'
            if next_value:
                day_string += str(next_value['영업일']) + '일'
            Day.append(day_string)

    # 배열의 마지막 요소들을 사용하여 문자열 생성
    잔존만기1 = str(data_[-2]['잔존만기'])
    잔존만기2 = str(data_[-1]['잔존만기'])

    callName1 = f" (<span style='color:greenyellow;'>{str(int(data_[-4]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {data_[-2]['영업일']} ]"
    callName2 = f" (<span style='color:greenyellow;'>{str(int(data_[-3]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {data_[-1]['영업일']} ]"
    putName1 = f" (<span style='color:greenyellow;'>{str(int(data_[-2]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {data_[-2]['영업일']} ]"
    putName2 = f" (<span style='color:greenyellow;'>{str(int(data_[-1]['거래대금'] / 100000000)):>4}억</span>) [ 영업 : {data_[-1]['영업일']} ]"
    
    
    result = {
        'series': [{
                'name': f'Call 잔존 : {잔존만기1} {callName1}',
                'data': data1,
                'color': '#FCAB2F',
                'yAxis': 0
            }, {
                'name': f'Put 잔존 : {잔존만기1} {putName1}',
                'data': data3,
                'color': '#00F3FF',
                'yAxis': 0
            }, {
                'name': f'Call 잔존 : {잔존만기2} {callName2}',
                'data': data2,
                'color': 'tomato',
            }, {
                'name': f'Put 잔존 : {잔존만기2} {putName2}',
                'data': data4,
                'color': 'dodgerblue',
            }], 
        'categories': Day
        }
        
    return result

async def create_ElwRatio_data():
    result = client['ELW']['ElwRatioData']
    data = pd.DataFrame(result.find({}, {'_id':0}).sort([('날짜', -1)]).limit(8))
    data = data.sort_values(by='날짜')
    data = data.to_dict(orient='records')
    call, put, category = [],[],[]

    for value in data:
        콜비율 = round(value['콜_거래대금'] / (value['콜_거래대금'] + value['풋_거래대금']) * 100, 2)
        풋비율 = round(value['풋_거래대금'] / (value['콜_거래대금'] + value['풋_거래대금']) * 100, 2)

        날짜문자열 = str(value['날짜'])
        날짜포맷 = 날짜문자열[4:6] + '.' + 날짜문자열[6:8]
        카테고리문자열 = f"{날짜포맷}<br><span style=\"color:#FCAB2F\">{콜비율} %</span><br><span style=\"color:#00F3FF\">{풋비율} %</span>"

        call.append(콜비율)
        put.append(풋비율)
        category.append(카테고리문자열)
    
    result = {
        'series': [{
                    'name': '콜',
                    'color': '#FCAB2F',
                    'pointPlacement': -0.08,
                    'pointWidth': 20, 
                    'grouping': False,
                    'data': call
                }, {
                    'name': '풋',
                    'color': '#00A7B3',
                    'pointPlacement': 0.08,
                    'pointWidth': 20,
                    'grouping': False,
                    'data': put
                }],
        'categories': category
    }
        
    return result

# Trend Table
async def create_trend_table_data():
    col = client.AoX.TrendData
    lit = list(col.find({},{'_id':0}))[-1]
    result = [{ '구분': '코스피200', '외국인': lit['외국인_코스피200'], '외국인_누적': lit['외국인_코스피200_누적'], '기관': lit['기관_코스피200'], '기관_누적': lit['기관_코스피200_누적'], '개인': lit['개인_코스피200'], '개인_누적': lit['개인_코스피200_누적'] },
        { '구분': '코스피', '외국인': lit['외국인_코스피'], '외국인_누적': lit['외국인_코스피_누적'], '기관': lit['기관_코스피'], '기관_누적': lit['기관_코스피_누적'], '개인': lit['개인_코스피'], '개인_누적': lit['개인_코스피_누적'] },
        { '구분': '코스닥', '외국인': lit['외국인_코스닥'], '외국인_누적': lit['외국인_코스닥_누적'], '기관': lit['기관_코스닥'], '기관_누적': lit['기관_코스닥_누적'], '개인': lit['개인_코스닥'], '개인_누적': lit['개인_코스닥_누적'] },
        { '구분': '선물', '외국인': lit['외국인_선물'], '외국인_누적': lit['외국인_선물_누적'], '기관': lit['기관_선물'], '기관_누적': lit['기관_선물_누적'], '개인': lit['개인_선물'], '개인_누적': lit['개인_선물_누적'] },
        { '구분': '콜옵션', '외국인': lit['외국인_콜옵션'], '외국인_누적': lit['외국인_콜옵션_누적'], '기관': lit['기관_콜옵션'], '기관_누적': lit['기관_콜옵션_누적'], '개인': lit['개인_콜옵션'], '개인_누적': lit['개인_콜옵션_누적'] },
        { '구분': '풋옵션', '외국인': lit['외국인_풋옵션'], '외국인_누적': lit['외국인_풋옵션_누적'], '기관': lit['기관_풋옵션'], '기관_누적': lit['기관_풋옵션_누적'], '개인': lit['개인_풋옵션'], '개인_누적': lit['개인_풋옵션_누적'] }]
    return result

async def get_WA1_data(req:Request):
    while True:
        is_disconnected = await req.is_disconnected()
        if is_disconnected: break
        
        col = client.ELW.Month1
        lit1 = list(col.find({},{'_id':0}))
        WA1 = await create_WA_data(lit1)
        col = client.ELW.Month2
        lit2 = list(col.find({},{'_id':0}))
        WA2 = await create_WA_data(lit2)
        
        result = {
            'WA1' : WA1,
            'WA2' : WA2,
            'TrendData' : await create_trend_table_data(),
        }
        
        json_data = json.dumps(result)
        yield f"data: {json_data}\n\n"
        await asyncio.sleep(120)

@router.get('/weightAvgPage1')
async def Stream_CTP(req : Request):
    response_stream = get_WA1_data(req)
    return StreamingResponse(response_stream, media_type='text/event-stream')


async def get_WA3_data(req:Request):
    while True:
        is_disconnected = await req.is_disconnected()
        if is_disconnected: break
        
        col = client.ELW.Month4
        lit = list(col.find({},{'_id':0}))
        WA3 = await create_WA_data(lit)
        DayGr = await create_DayGr_data()
        ElwRatioData = await create_ElwRatio_data()
        
        result = {
            'WA3' : WA3, 
            'DayGr' : DayGr,
            'ElwRatioData' : ElwRatioData
        }
        
        json_data = json.dumps(result)
        yield f"data: {json_data}\n\n"
        await asyncio.sleep(120)

@router.get('/weightAvgPage2')
async def Stream_CTP(req : Request):
    response_stream = get_WA3_data(req)
    return StreamingResponse(response_stream, media_type='text/event-stream')







@router.get('/{name}')
async def loadDB(name):
    try :
        col = client['ELW'][name]
        data = pd.DataFrame(col.find({},{'_id':0}))
        data = data.fillna(0)
        return data.to_dict(orient='records')
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})