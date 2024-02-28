from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta, time as dt_time
import pymongo
import pandas as pd
import numpy as np
import logging
import Api.hts as hts
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

def 경과일범위(x): 
    current_date = datetime.now().date()
    year = int(f"20{x[:3]}")
    month = int(x[3])
    if month == 1 :
        listed_date_1 = datetime(year,1,1).date()
        listed_date_2 = datetime(year,3,31).date()
    elif month == 2 :
        listed_date_1 = datetime(year,4,1).date()
        listed_date_2 = datetime(year,6,30).date()
    elif month == 3 :
        listed_date_1 = datetime(year,7,1).date()
        listed_date_2 = datetime(year,9,30).date()
    else :
        listed_date_1 = datetime(year,10,1).date()
        listed_date_2 = datetime(year,12,31).date()
    start = (current_date - listed_date_1).days
    end = (current_date - listed_date_2).days
    return f'{start} ~ {end}'

class IPO_Pulse:
    def __init__(self):
        today = datetime.today()
        start_day = today - timedelta(days=365*3)
        st_date = datetime(start_day.year, self.set_quarter(start_day.month), 1 )
        col = client.HTS.Financial
        self.fin= list(col.find({ }, {'_id' : 0}))[0]
        self.st_date = st_date
        self.base_data = self.base()
        
    def set_quarter(self, month):
        if 1 <= month <= 3:
            return 1
        elif 4 <= month <=6 :
            return 4
        elif 7 <= month <=9:
            return 7
        else : 
            return 10

    def 경과일수(self, x): 
        current_date = datetime.now().date()
        listed_date = datetime.strptime(x, '%Y-%m-%d').date()
        days_passed = (current_date - listed_date).days
        return days_passed
    
    def get_quarter(self, date_str):
        year, month, _ = map(int, date_str.split('-'))
        if 1 <= month <= 3:
            return f"{str(year)[2:]} 1Q"
        elif 4 <= month <= 6:
            return f"{str(year)[2:]} 2Q"
        elif 7 <= month <= 9:
            return f"{str(year)[2:]} 3Q"
        else:
            return f"{str(year)[2:]} 4Q"
    
    def base(self):
        col = client['Info']['StockPriceDaily']
        StockPriceDaily = pd.DataFrame(col.find({},{'_id':0, '종목코드':1, '현재가':1, '등락률':1}))
        
        col = client['Info']['IpoPulse']
        IpoPulse = pd.DataFrame(col.find({ '상장예정일' : { "$gt" : self.st_date.strftime("%Y-%m-%d") } },{'_id':0 }))
        
        col = client['Info']['StockEtcInfo']
        StockEtcInfo = pd.DataFrame(col.find({ },{'_id':0, '종목코드':1, 'PBR' : 1 }))
        
        data = IpoPulse.merge(StockPriceDaily, on='종목코드', how='left').merge(StockEtcInfo, on='종목코드')
        data['경과일수'] = data['상장예정일'].apply(lambda x : self.경과일수(x))
        data['최고가대비'] = round((data['현재가'] - data['최고가']) / data['최고가'] * 100, 1)
        data['공모가대비'] = round((data['현재가'] - data['공모가']) / data['공모가'] * 100, 1)
        
        for column in ['최고가', '최저가', '현재가', '최고가대비', '공모가대비', 'PBR']:
            data[column].replace([np.inf, -np.inf], np.nan, inplace=True)  # inf를 NaN으로 변환
            data[column].fillna(0, inplace=True)  # NaN을 0으로 변환
        for column in ['최고가', '최저가', '현재가', '최고가대비', '공모가대비', 'PBR']:
            data[column] = data[column].astype(float)

        data['id'] = data.index
        data = data.fillna('-')
        return data
    
    def 분기별로구분(self):
        # 분기별로 데이터를 그룹화
        grouped_data = {}
        for item in self.base_data.to_dict(orient='records'):
            quarter = self.get_quarter(item['상장예정일'])
            if quarter not in grouped_data:
                grouped_data[quarter] = {
                    'Data' : []
                }
            grouped_data[quarter]['Data'].append(item)
        dict_keys = list(grouped_data.keys())
        for item in dict_keys :
            grouped_data[item]['Length'] = len(grouped_data[item]['Data'])
            grouped_data[item]['Days'] = f'{경과일범위(item)}'
        
        return grouped_data
    
    def table_data(self):
        return self.base_data
    
    def financial(self):
        dict_keys = list(self.fin)
        result = []
        for item in dict_keys:
            result += self.fin[item]
        return list(set(result))

@router.get('/chart')
async def IPO_Pulse_():
    try :
        ipo = IPO_Pulse()
        chart = ipo.분기별로구분()
        return chart
    
    except Exception as e:
        logging.error('post(/data)\n', e)
        return HTTPException(status_code=500, detail=str(e))

# 하위 업종 > 선택시 POST
@router.post('/data')
async def IPO_Pulse_(req : Request):
    try :
        ipo = IPO_Pulse()
        
        req_data = await req.json()
        high = req_data['high']
        start = req_data['start']
        day = req_data['day']
        selected = req_data['selected']
        finance = req_data['finance']
        order = req_data['order']
        lockUp = req_data['lockUp']
        industry = req_data['industry']
        # print(datetime.today().strftime('%y-%m-%d %H:%M:%S'),high, start, day, selected, finance)

        origin = ipo.table_data()
        table = origin.copy()

        chart = ipo.분기별로구분()
        
        if order != None :
            table.sort_values(by='PBR', inplace=True)
        if selected != None :
            table = pd.DataFrame(chart[selected]['Data'])
        if finance != None :
            code_list = ipo.financial()
            table  = table[table['종목코드'].isin(code_list)]
        if high != None :
            table = table[ (int(high[1]) <= table['최고가대비'] ) & ( table['최고가대비'] <= int(high[0])) ]
        if start != None : 
            table = table[ (int(start[1]) <= table['공모가대비'] ) & ( table['공모가대비'] <= int(start[0])) ]
        if day != None :
            table = table[ (int(day[0]) <= table['경과일수']) & ( table['경과일수'] <= int(day[1])) ]
        if lockUp != None :
            table = table[table['보호예수카운트'] != '']
            table['보호예수카운트'] = table['보호예수카운트'].astype(int)
            table = table[ (int(lockUp[0]) <= table['보호예수카운트']) & ( table['보호예수카운트'] <= int(lockUp[1])) ]
        if industry != None and len(industry)>0 :
            table = table[table['업종명'].isin(industry)]
        
        table['상장예정일'] = table['상장예정일'].str[2:]        
        result = {
            'table': table.to_dict(orient='records') if table is not None else [],
            'industry' : hts.countIndustry(origin),
            'total' : len(origin)
        }
        
        return JSONResponse(content=result)
    
    except Exception as e:
        logging.error('post(/data)\n', e)
        return HTTPException(status_code=500, detail=str(e))





# @router.get('/{name}')
# async def loadDB(name):
#     try :
#         result = client['Etc'][name.title()]
#         return list(result.find({}, {'_id':False}))
#     except Exception as e:
#         logging.error(e)
#         return JSONResponse(status_code=500, content={"message": "Internal Server Error"})