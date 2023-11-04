import requests
from json.decoder import JSONDecodeError
import pandas as pd
import time
import datetime
from tqdm import tqdm
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
def getData(code):
    api_url = os.getenv("API_URL_GET_STOCK")
    requestData = requests.get(f'{api_url}/{code}')
    return pd.DataFrame(requestData.json())

def run():
    api_url = os.getenv("API_URL_STOCK_PRICE_DAILY_LIST")
    requestData = requests.get(api_url)
    data = pd.DataFrame(requestData.json())
    data = data[data['업종명'] != '기타']
    codeList = data['종목코드'].tolist()
    
    오늘날짜 = datetime.datetime.now().date() # 현재 날짜와 시간 정보를 가져온 후, 년, 월, 일 
    today_date = datetime.datetime(오늘날짜.year, 오늘날짜.month, 오늘날짜.day) # 년, 월, 일 정보만 가진 datetime.datetime 객체를 생성

    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Stock']
        for idx, row in tqdm(data.iterrows()):
            종목코드 = row['종목코드']
            try:
                db = client['Stock']
                collection = db[종목코드]
                
                existing_data = collection.find_one({"날짜": today_date})
                data_to_insert = {
                    "날짜": today_date,
                    "시가": row['시가'],
                    "고가": row['고가'],
                    "저가": row['저가'],
                    "종가": row['현재가'],
                    "거래량": row['거래량']
                }

                if existing_data:
                    collection.update_one({"날짜": today_date}, {"$set": data_to_insert})
                else:
                    collection.insert_one(data_to_insert)
            except Exception as e:
                print('stock_get_price : 거래량 DB저장 실패 :', e)
        
        # # 각 종목별로 데이터를 가져와서 MongoDB에 저장
        # for code in tqdm(codeList):
        #     try :
        #         df = getData(code)  # 데이터 가져오기
        #         time.sleep(0.5)
        #         collection = db[code]  # MongoDB에서 해당 종목 코드의 컬렉션
        #         records = df.to_dict('records')  # DataFrame을 Dictionary 형태로 변환
        #         collection.insert_many(records)  # MongoDB에 데이터 저장
        #     except JSONDecodeError:
        #         print(f"JSON decoding error for code: {code}")
        #         continue