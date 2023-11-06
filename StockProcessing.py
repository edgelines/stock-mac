from multiprocessing import Pool, cpu_count
import requests
import pandas as pd
from utils import utils, send
import os
from dotenv import load_dotenv
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
                   'DMI_3', 'DMI_4', 'DMI_5']
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    
    send.data(df.to_json(orient='records', force_ascii=False))
    

if __name__ == '__main__':
    run()