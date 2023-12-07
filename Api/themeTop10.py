from fastapi import APIRouter, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import pymongo
import json
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

@router.get('/Industry')
async def IndustryThemeTop10():
    try :
        col = client['Industry']['Rank']
        data = pd.DataFrame(col.find({},{'_id' :0, '전체' : 0, '상승' : 0, '보합' : 0, '하락' : 0, '등락그래프' : 0, '상승%' : 0, '순위' : 0}))
        
        col2 = client['ABC']['stockSectorsThemes']
        data2 = pd.DataFrame(col2.find({},{'_id' :0}))
        
        # 조건명 업종이 전일대비 등락률 0% 이상
        data = data[data['전일대비'] > 0]
        filtered_df = data2[data2['업종명'].isin(data['업종명'])]
        # 테마명의 출현 빈도 계산
        theme_count = pd.Series([theme for sublist in filtered_df['테마명'] for theme in sublist]).value_counts()
        # 상위 10개 테마 추출
        top_10_themes = theme_count.head(10).reset_index()
        top_10_themes.columns = ['theme', 'count']
        
        # 탑10 테마리스트
        top_themes = top_10_themes['theme'].to_list()
        col3 = client['Info']['ThemeStocksCollection']
        data3 = list(col3.find({},{'_id' :0}))
        
        # 각테마에 해당하는 종목들 가져옴
        top_theme_items = [item for item in data3 if item['테마명'] in top_themes]
        
        col4 = client['AoX']['StockPriceDailyList']
        data4 = pd.DataFrame(col4.find({},{'_id' :0, '거래량':0, '전일거래량':0, '현재가' : 0, '고가' :0, '시가' :0, '저가':0}))
        
        top_2_by_theme = []
        for theme_data in top_theme_items:
            # 현재 테마의 종목코드 리스트
            theme_stock_codes = [item['종목코드'] for item in theme_data['data']]

            # StockPrice에서 현재 테마의 종목만 필터링
            theme_stocks = data4[data4['종목코드'].isin(theme_stock_codes)]

            # 등락률에 따라 상위 2개 종목 추출
            top_2_stocks = theme_stocks.nlargest(2, '등락률')

            # 결과에 추가
            top_2_by_theme.append({
                'theme': theme_data['테마명'],
                'items': top_2_stocks.to_dict(orient='records')
            })
        top_2_by_theme = pd.DataFrame(top_2_by_theme)
        
        result = pd.merge(top_10_themes, top_2_by_theme, on='theme')
        return result.to_dict(orient='records')

    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/Stocks')
async def StocksThemeTop10():
    try :
        print()
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})