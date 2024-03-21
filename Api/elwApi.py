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
    
    # data = pd.DataFrame(data)
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

# @router.get('/{name}')
# async def loadDB(name):
#     try :
#         col = client['ELW'][name]
#         data = pd.DataFrame(col.find({},{'_id':0}))
#         data = data.fillna(0)
#         return data.to_dict(orient='records')
#     except Exception as e:
#         logging.error(e)
#         return JSONResponse(status_code=500, content={"message": "Internal Server Error"})