import requests
from json.decoder import JSONDecodeError
import pandas as pd
import time
from multiprocessing import Pool, cpu_count
from datetime import datetime
from tqdm import tqdm
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from utils import send
from utils import utils

load_dotenv()
def getData(code):
    api_url = os.getenv("API_URL_GET_STOCK")
    requestData = requests.get(f'{api_url}/{code}')
    return pd.DataFrame(requestData.json())

def marketMA(today_date, data):
    # 카운트 초기화
    summary = {
        '코스피_전체': 0, '코스피_위_MA50': 0, '코스피_위_MA112': 0, '코스피_위_MA20': 0,
        '코스닥_전체': 0, '코스닥_위_MA50': 0, '코스닥_위_MA112': 0, '코스닥_위_MA20': 0,
        '코스피200_전체': 0, '코스피200_위_MA50': 0, '코스피200_위_MA112': 0, '코스피200_위_MA20': 0
    }
    
    api_url = os.getenv("API_URL_MARKET_LIST")
    requestData = requests.get(api_url)
    tmp = requestData.json()[0]
    코스피200_코드 = tmp['코스피200']
    코스피_코드 = tmp['코스피']
    코스닥_코드 = tmp['코스닥']

    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Stock']
        for idx, row in tqdm(data.iterrows()):
            종목코드 = row['종목코드']
            try:
                db = client['Stock']
                collection = db[종목코드]

                current_price = row['현재가']
                try :
                    stock_data = pd.DataFrame(collection.find({}, {'_id': False}))
                    # 20일 MA와 50일 MA와 112일 MA 계산
                    ma20 = stock_data['종가'].rolling(window=20).mean().iloc[-1]
                    ma50 = stock_data['종가'].rolling(window=50).mean().iloc[-1]
                    ma112 = stock_data['종가'].rolling(window=112).mean().iloc[-1]

                    # 현재가와 비교
                    is_above_ma20 = current_price > ma20
                    is_above_ma50 = current_price > ma50
                    is_above_ma112 = current_price > ma112
                    # 카운트 업데이트
                    if 종목코드 in 코스피200_코드:
                        summary['코스피200_전체'] += 1
                        if is_above_ma20:
                            summary['코스피200_위_MA20'] += 1
                        if is_above_ma50:
                            summary['코스피200_위_MA50'] += 1
                        if is_above_ma112:
                            summary['코스피200_위_MA112'] += 1

                    if 종목코드 in 코스피_코드:
                        summary['코스피_전체'] += 1
                        if is_above_ma20:
                            summary['코스피_위_MA20'] += 1
                        if is_above_ma50:
                            summary['코스피_위_MA50'] += 1
                        if is_above_ma112:
                            summary['코스피_위_MA112'] += 1

                    elif 종목코드 in 코스닥_코드:
                        summary['코스닥_전체'] += 1
                        if is_above_ma50:
                            summary['코스닥_위_MA20'] += 1
                        if is_above_ma50:
                            summary['코스닥_위_MA50'] += 1
                        if is_above_ma112:
                            summary['코스닥_위_MA112'] += 1
                except : pass
                        
            except Exception as e:
                send.errors('Mac > StockUpdate>Processing :', e)
    # 결과를 DataFrame으로 변환
    tmp = pd.DataFrame({
        '날짜': [today_date],
        **summary
    })

    try :
        send.data(tmp.to_json(orient='records', force_ascii=False), 'IndexMA')
    except Exception as e:
        send.errors('Mac > StockUpdate,MarketMA :', e)

def DMI_Rolling(args):
    today_date, row = args
    종목코드 = row['종목코드']
    
    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Stock']    
        collection = db[종목코드]
        existing_data = collection.find_one({"날짜": today_date})
            
        data_to_insert = {
            "날짜": [today_date],
            "시가": [row['시가']],
            "고가": [row['고가']],
            "저가": [row['저가']],
            "종가": [row['현재가']],
            "거래량": [row['거래량']],
        }
        
        n_list = [3, 4, 5, 6, 7]
        
        try :
            df = pd.DataFrame(collection.find({}, {'_id': 0, '날짜': 1, '시가': 1, '고가': 1, '저가': 1, '종가': 1, '거래량': 1}).sort('날짜', -1).limit(8))
            # df['날짜'] = pd.to_datetime(df['날짜'])
            df = df.sort_values(by='날짜').reset_index(drop=True)
            df_insert = pd.DataFrame(data_to_insert)
            df = pd.concat([df, df_insert]).reset_index(drop=True)
                
            dmi_results_1 = utils.cal_DMI_rolling(df, n_list, method='가중')
            dmi_results_1 = dmi_results_1.fillna('-')
            last_row = df.iloc[-1].to_dict()
            if existing_data:
                collection.update_one({"날짜": today_date}, {"$set": last_row})
            else:
                collection.insert_one(last_row)
        except :
            print(종목코드, ' : 신규상장주식')
            df = pd.DataFrame(collection.find({}, {'_id': 0, '날짜': 1, '시가': 1, '고가': 1, '저가': 1, '종가': 1, '거래량': 1}))
            # df['날짜'] = pd.to_datetime(df['날짜'])
            # df = df.sort_values(by='날짜').reset_index(drop=True)
            df_insert = pd.DataFrame(data_to_insert)
            df = pd.concat([df, df_insert]).reset_index(drop=True)
                
            dmi_results_1 = utils.cal_DMI_rolling(df, n_list, method='가중')
            dmi_results_1 = dmi_results_1.fillna('-')
            last_row = df.iloc[-1].to_dict()
            if existing_data:
                collection.update_one({"날짜": today_date}, {"$set": last_row})
            else:
                collection.insert_one(last_row)
            
            
    # with MongoClient('mongodb://localhost:27017/') as client:
    #     db = client['Stock']
    #     for idx, row in tqdm(data.iterrows()):
    #         종목코드 = row['종목코드']
    #         db = client['Stock']
    #         collection = db[종목코드]
    #         existing_data = collection.find_one({"날짜": today_date})
            
    #         data_to_insert = {
    #             "날짜": [today_date],
    #             "시가": [row['시가']],
    #             "고가": [row['고가']],
    #             "저가": [row['저가']],
    #             "종가": [row['현재가']],
    #             "거래량": [row['거래량']],
    #         }
            
    #         n_list = [3, 4, 5, 6, 7]
            
    #         df = pd.DataFrame(collection.find({}, {'_id': 0, '날짜': 1, '시가': 1, '고가': 1, '저가': 1, '종가': 1, '거래량': 1}).sort('날짜', -1).limit(8))
    #         df = df.sort_values(by='날짜').reset_index(drop=True)
    #         df_insert = pd.DataFrame(data_to_insert)
    #         df = pd.concat([df, df_insert]).reset_index(drop=True)
            
    #         dmi_results_1 = utils.cal_DMI_rolling(df, n_list, method='가중')
    #         dmi_results_1 = dmi_results_1.fillna('-')
    #         last_row = df.iloc[-1].to_dict()
    #         if existing_data:
    #             collection.update_one({"날짜": today_date}, {"$set": last_row})
    #         else:
    #             collection.insert_one(last_row)

def run():
    api_url = os.getenv("API_URL_STOCK_PRICE_DAILY_LIST")
    requestData = requests.get(api_url)
    data = pd.DataFrame(requestData.json())
    data = data[data['업종명'] != '기타']
    
    오늘날짜 = datetime.now().date() # 현재 날짜와 시간 정보를 가져온 후, 년, 월, 일 
    today_date = datetime(오늘날짜.year, 오늘날짜.month, 오늘날짜.day) # 년, 월, 일 정보만 가진 datetime.datetime 객체를 생성

    marketMA(today_date, data)

    pool = Pool(cpu_count()) # 사용 가능한 모든 CPU 코어를 사용하여 Pool을 생성
    pool.map(DMI_Rolling, [(today_date, row) for _, row in data.iterrows()])
    pool.close()
    pool.join()
    # with MongoClient('mongodb://localhost:27017/') as client:
    #     db = client['Stock']
    #     for idx, row in tqdm(data.iterrows()):
    #         종목코드 = row['종목코드']
    #         db = client['Stock']
    #         collection = db[종목코드]
    #         existing_data = collection.find_one({"날짜": today_date})
            
    #         data_to_insert = {
    #             "날짜": [today_date],
    #             "시가": [row['시가']],
    #             "고가": [row['고가']],
    #             "저가": [row['저가']],
    #             "종가": [row['현재가']],
    #             "거래량": [row['거래량']],
    #         }
            
    #         n_list = [3, 4, 5, 6, 7]
            
    #         df = pd.DataFrame(collection.find({}, {'_id': 0, '날짜': 1, '시가': 1, '고가': 1, '저가': 1, '종가': 1, '거래량': 1}).sort('날짜', -1).limit(8))
    #         df = df.sort_values(by='날짜').reset_index(drop=True)
    #         df_insert = pd.DataFrame(data_to_insert)
    #         df = pd.concat([df, df_insert]).reset_index(drop=True)
            
    #         dmi_results_1 = utils.cal_DMI_rolling(df, n_list, method='가중')
    #         dmi_results_1 = dmi_results_1.fillna('-')
    #         last_row = df.iloc[-1].to_dict()
    #         if existing_data:
    #             collection.update_one({"날짜": today_date}, {"$set": last_row})
    #         else:
    #             collection.insert_one(last_row)
                
                
                