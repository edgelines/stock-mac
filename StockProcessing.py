import os
import requests
import pandas as pd
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from utils import utils, send
import warnings
warnings.filterwarnings('ignore')


def run():
    load_dotenv()
    api_url = os.getenv("API_URL_STOCK_PRICE_DAILY_LIST")
    # 데이터 로드
    requestData = requests.get(api_url)
    data = pd.DataFrame(requestData.json())
    data = data[data['업종명'] != '기타']
    
    # 멀티프로세싱 Pool 생성
    pool = Pool(cpu_count()) # 사용 가능한 모든 CPU 코어를 사용하여 Pool을 생성
    stockIndicators_list = pool.map(utils.calculate_for_ticker, [row for _, row in data.iterrows()])
    
    # None 값을 제거합니다.
    stockIndicators_list = [x for x in stockIndicators_list if x is not None]
    pool.close()
    pool.join()
    
    df = pd.DataFrame(stockIndicators_list)
    df = df.fillna(0)
    df = df[(df['시가'] > 0) & (df['저가'] > 0) & (df['종가'] > 0) & (df['고가'] > 0)]
    numeric_columns = ['시가', '고가', '저가', '종가', '거래량', 
                   'willR_5', 'willR_7', 'willR_14', 'willR_20', 'willR_33', 
                   'DMI_3', 'DMI_4', 'DMI_5', 'DMI_6', 'DMI_7']
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    
    with MongoClient('mongodb://localhost:27017/') as client:
        collection = client['Search']['StockFinance']
        finance = pd.DataFrame(collection.find({}, {'_id':False}))
        df = pd.merge(df, finance[['티커', '유보율', '부채비율']], on='티커', how='left')
        df = df.fillna(0)
        send.data(df.to_json(orient='records', force_ascii=False), 'StockSearch')
    
        오늘날짜 = datetime.now().date() # 현재 날짜와 시간 정보를 가져온 후, 년, 월, 일 
        today_date = datetime(오늘날짜.year, 오늘날짜.month, 오늘날짜.day) # 년, 월, 일 정보만 가진 datetime.datetime 객체를 생성
        tmp = df[( df['willR_5'] < -90) & (df['willR_7'] < -90) & (df['willR_14'] < -90) & (df['willR_20'] < -90) & (df['willR_33'] < -90) 
                & (df['DMI_3'] < 10) & (df['DMI_4'] < 10) & (df['DMI_5'] < 10) ]
                # & (df['DMI_3'] < 10) & (df['DMI_4'] < 10) & (df['DMI_5'] < 10) & (df['DMI_6'] < 10) & (df['DMI_7'] < 10)]
        tmp['조건일'] = today_date
        tmp['현재가'] = tmp['종가']
    
    # with MongoClient('mongodb://localhost:27017/') as client:
        collection = client['Search']['Tracking']
        # MongoDB에 upsert 수행
        operations = []
        for _, row in tmp.iterrows():
            # 문서가 이미 존재하면 '현재가'를 업데이트, 존재하지 않으면 새로운 문서를 삽입
            operation = UpdateOne(
                {'티커': row['티커'], '조건일': row['조건일']},  # 매칭 조건
                {
                    '$set': {'현재가': row['종가']},  # 항상 업데이트 될 내용
                    '$setOnInsert': {  # 문서가 삽입될 때만 설정될 내용
                        '티커': row['티커'],
                        '업종명': row['업종명'],
                        '종목명': row['종목명'],
                        '종가': row['종가'],
                        '조건일': row['조건일'], 
                        '유보율': row['유보율'], 
                        '부채비율': row['부채비율'], 
                    }
                },
                upsert=True  # 문서가 없으면 삽입
            )
            operations.append(operation)

        # Bulk write를 사용하여 모든 연산 실행
        if operations:
            collection.bulk_write(operations, ordered=False)

        try :
            # stockIndicators_list에서 각 티커별 최신 '현재가'를 추출합니다.
            latest_prices = {item['티커']: item['종가'] for item in stockIndicators_list}

            # MongoDB에 업데이트 수행
            operations = []
            for ticker, current_price in latest_prices.items():
                operation = UpdateOne(
                    {'티커': ticker},  # 해당 티커에 대응하는 모든 문서를 선택
                    {'$set': {'현재가': current_price}},  # '현재가'만 업데이트
                    upsert=False  # 존재하는 문서에 대해서만 업데이트 (삽입은 하지 않음)
                )
                operations.append(operation)

            # MongoDB의 'Tracking' 컬렉션에 Bulk write를 사용하여 모든 업데이트 실행
            collection = client['Search']['Tracking']
            if operations:
                collection.bulk_write(operations, ordered=False)
        except Exception as e:
            print('새로운 종목 저장 실패', e)

if __name__ == '__main__':
    run()