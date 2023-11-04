from multiprocessing import Pool, cpu_count
import requests
import pandas as pd
from utils import utils
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

# if __name__ == '__main__':