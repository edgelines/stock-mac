from fastapi import APIRouter, Request
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
# url : /industryChartData/

def get_top_themes(rankName, StockSectorsThemes, StockThemes, 종목등락률, 추출테마수):
    sectorsData = [row for row in StockSectorsThemes if row['업종명'] in rankName and row['등락률'] >= 종목등락률]

    themeRankInfoByTheme = {themeInfo['테마명']: themeInfo for themeInfo in StockThemes}

    themeStats = {}
    for row in sectorsData:
        for theme in row['테마명']:
            themeFluctuation = themeRankInfoByTheme[theme]['등락률'] if theme in themeRankInfoByTheme else 0

            if theme not in themeStats:
                themeStats[theme] = {
                    'count': 1,
                    'fluctuation': themeFluctuation,
                    'stocks': [{
                        'item': row['종목명'],
                        'changeRate': row['등락률'],
                        'volume': row['전일대비거래량'],
                        '종목코드': row['종목코드'],
                        '업종명': row['업종명']
                    }]
                }
            else:
                themeStats[theme]['count'] += 1
                themeStats[theme]['stocks'].append({
                    'item': row['종목명'],
                    'changeRate': row['등락률'],
                    'volume': row['전일대비거래량'],
                    '종목코드': row['종목코드'],
                    '업종명': row['업종명']
                })

    sortedThemes = sorted(themeStats.items(), key=lambda x: (x[1]['count'], x[1]['fluctuation']), reverse=True)

    top10Themes = [{
        'theme': theme,
        'count': stats['count'],
        'fluctuation': stats['fluctuation'],
        'stocks': sorted(stats['stocks'], key=lambda x: x['changeRate'], reverse=True)
    } for theme, stats in sortedThemes[:추출테마수]]

    return top10Themes

@router.post('/getThemes', response_class=JSONResponse)
async def GetThemes( request:Request ):
    req_data = await request.json() # post로 받은 데이터
    # req_data = json.dumps(req)

    checkboxStatusUp = req_data['checkboxStatusUp']
    checkboxStatusTup = req_data['checkboxStatusTup']
    checkboxStatusDown = req_data['checkboxStatusDown']
    rankRange = req_data['rankRange']
    # rankRange = json.dumps(req_data['rankRange'])
    
    col_테마 = client['Info']['StockThemes']
    테마 = pd.DataFrame(col_테마.find({},{'_id' : 0, '업종명' : 0, '종목코드' : 0}))
    collection = client['Info']['StockPriceDaily']
    results_df = pd.DataFrame(collection.find({},{'_id' : 0}))
    df_temp = pd.merge(results_df, 테마, on='종목명')
    StockSectorsThemes = df_temp.to_dict(orient='records')
    
    col = client['Themes']['Rank']        
    StockThemes = list(col.find({},{'_id' :0}))
    종목등락률=1
    추출테마수=10
    try :
        col = client.AoX.stockSectorsGR
        raw_data = list(col.find({}, {'_id': 0}))

        # 클라이언트 측에서 예상하는 데이터 구조로 변환
        df = [
            {
                "업종명": item['업종명'],
                "NOW": item['NOW'],
                "TOM": item['TOM'],
                "B1": item.get('B-1', None),
                "data": [item.get(f'B-{i}', None) for i in range(7, 0, -1)] + [item['TOM'], item['NOW']]  # B-7부터 B-0(NOW)까지
            } for item in raw_data
        ]
        
        업종그룹 = [
            ['디스플레이장비및부품', '반도체와반도체장비', '자동차', '자동차부품', '화학'],
            ['에너지장비및서비스', '전기장비', '전기제품', '전자장비와기기', '전자제품'],
            ['IT서비스', '게임엔터테인먼트', '소프트웨어', '방송과엔터테인먼트', '핸드셋'],
            ['컴퓨터와주변기기', '무역회사와판매업체', '무선통신서비스', '다각화된통신서비스', '디스플레이패널'],
            ['석유와가스', '가스유틸리티', '조선', '항공화물운송과물류', '해운사'],
            ['건설', '건축자재', '건축제품', '기계', '철강'],
            ['운송인프라', '도로와철도운송', '비철금속', '우주항공과국방', '통신장비'],
            ['부동산', '상업서비스와공급품', '은행', '증권', '창업투자'],
            ['가구', '가정용기기와용품', '인터넷과카탈로그소매', '가정용품', '판매업체'],
            ['생명과학도구및서비스', '생물공학', '제약'],
            ['건강관리기술', '건강관리장비와용품', '건강관리업체및서비스'],
            ['식품', '식품과기본식료품소매', '음료', '종이와목재', '포장재'],
            ['광고', '교육서비스', '양방향미디어와서비스', '화장품'],
            ['레저용장비와제품', '백화점과일반상점', '섬유', '항공사', '호텔']
        ]
        def 업종별데이터그룹화(data, 업종그룹):
            grouped_data = []
            for group in 업종그룹:
                filtered_group = [item for item in data if item['업종명'] in group]
                grouped_data.append(filtered_group)
            return grouped_data

        그룹화된데이터 = 업종별데이터그룹화(df, 업종그룹)

        stockSectorsChartData = { '반도체1': 그룹화된데이터[0], '반도체2': 그룹화된데이터[1], 'IT1': 그룹화된데이터[2], 'IT2': 그룹화된데이터[3], '조선': 그룹화된데이터[4], '건설1': 그룹화된데이터[5], '건설2': 그룹화된데이터[6], '금융': 그룹화된데이터[7], 'B2C': 그룹화된데이터[8], 'BIO1': 그룹화된데이터[9], 'BIO2': 그룹화된데이터[10], '식품': 그룹화된데이터[11], '아웃도어1': 그룹화된데이터[12], '아웃도어2': 그룹화된데이터[13], }
        
        allSectorNames = set()
        sectorNames = {}
        mergedFilteredData = {}
        topThemes = {}
        for rank in rankRange:
            filteredDataUp = {}
            filteredDataDown = {}
            sectorNamesForRank = set()

            for key in stockSectorsChartData:
                # 각 키에 대한 데이터셋에 대해 필터링 수행
                filteredDataUp[key] = [
                    item for item in stockSectorsChartData[key]
                    if (checkboxStatusUp[rank] and
                        rankRange[rank][0] <= item['NOW'] <= rankRange[rank][1] and
                        item['B1'] >= item['NOW'] and
                        (checkboxStatusTup[rank] == False or item['TOM'] >= item['NOW']))
                ]
                filteredDataDown[key] = [
                    item for item in stockSectorsChartData[key]
                    if (checkboxStatusDown[rank] and
                        rankRange[rank][0] <= item['NOW'] <= rankRange[rank][1] and
                        item['B1'] < item['NOW'] and
                        (checkboxStatusTup[rank] == False or item['TOM'] >= item['NOW']))
                ]
            
            for key in stockSectorsChartData:
                    mergedFilteredData[key] = mergedFilteredData.get(key, []) + filteredDataUp.get(key, []) + filteredDataDown.get(key, [])
            
            # 업종명 추출
            for key in mergedFilteredData:
                data = mergedFilteredData[key]
                for row in data:
                    if '업종명' in row:
                        sectorNamesForRank.add(row['업종명'])

            # 최종 결과 저장
            uniqueSectorNamesForRank = sectorNamesForRank - allSectorNames
            sectorNames[rank] = list(uniqueSectorNamesForRank)
            allSectorNames.update(sectorNamesForRank)
            
            topThemes[rank] = get_top_themes(allSectorNames, StockSectorsThemes, StockThemes, 종목등락률, 추출테마수)
            
        result = {
            'origin' : stockSectorsChartData,
            'industryGr' : mergedFilteredData,
            'industryName' : allSectorNames,
            'topThemes' : topThemes
        }
        
        return result
        
    except Exception as e:
        return {"error" : str(e)}

def 상위10개테마추출(df):
    # 테마명의 출현 빈도 계산
    theme_count = pd.Series([theme for sublist in df['테마명'] for theme in sublist]).value_counts()
    # 상위 10개 테마 추출
    top_10_themes = theme_count.head(10).reset_index()
    top_10_themes.columns = ['theme', 'count']

    # 탑10 테마리스트
    top_themes = top_10_themes['theme'].to_list()
    ThemeStocksCollection = client.Info.ThemeStocksCollection
    data3 = list(ThemeStocksCollection.find({},{'_id' :0}))

    # 각테마에 해당하는 종목들 가져옴
    top_theme_items = [item for item in data3 if item['테마명'] in top_themes]

    현재가 = client.Info.StockPriceDaily
    data4 = pd.DataFrame(현재가.find({},{'_id' :0, '거래량':0, '전일거래량':0, '현재가' : 0, '고가' :0, '시가' :0, '저가':0}))

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

def filtered_rows(filtered_data, volume_range, reserve_ratio, ratio_range, volume_avg, market_cap, debt_ratio, order, order_by, ABC):
    filtered_rows = [row for row in filtered_data if
                    int(row['거래량평균%']) >= volume_range[0] and
                    int(row['거래량평균%']) <= volume_range[1] and
                    float(row['유보율']) >= reserve_ratio and
                    float(row['종목 등락률']) >= ratio_range[0] and
                    float(row['종목 등락률']) <= ratio_range[1] and
                    float(row['5일 평균거래량']) >= volume_avg and
                    float(row['시가총액']) >= market_cap[0] and
                    float(row['시가총액']) <= market_cap[1] and
                    float(row['부채비율']) <= debt_ratio]

    sorted_rows = sorted(filtered_rows, key=lambda x: x[order_by], reverse=(order == 'desc'))
    
    for row in sorted_rows:
        row['M1'] = 'O' if any(obj['주도주 1순위'] == row['종목명'] for obj in ABC[0]['data']) else ''
        row['M2'] = 'O' if any(obj['주도주 2순위'] == row['종목명'] for obj in ABC[1]['data']) else ''
    
    return sorted_rows

@router.post('/getStocks', response_class=JSONResponse)
async def GetStocks( request:Request ):
    req_data = await request.json() # post로 받은 데이터

    volumeRange = req_data['volumeRange']
    reserveRatio = req_data['reserveRatio']
    ratioRange = req_data['ratioRange']
    volumeAvg = req_data['volumeAvg']
    marketCap = req_data['marketCap']
    debtRatio = req_data['debtRatio']

    try :
        # 업종순위
        col = client['Industry']['Rank']
        data = pd.DataFrame(col.find({},{'_id' :0, '전체' : 0, '상승' : 0, '보합' : 0, '하락' : 0, '등락그래프' : 0, '상승%' : 0, '순위' : 0}))
        
        col2 = client['Info']['IndustryThemes']
        # col2 = client['ABC']['stockSectorsThemes']
        data2 = pd.DataFrame(col2.find({},{'_id' :0}))
        
        # 조건명 업종이 전일대비 등락률 0% 이상
        data = data[data['전일대비'] > 0]
        filtered_df = data2[data2['업종명'].isin(data['업종명'])]

        # M1-M2 Table
        col = client.ABC.themeBySecByItem
        ABC = list(col.find({},{'_id' : 0}))
        
        col = client.ABC.stockPrice
        df = pd.DataFrame(col.find({},{'_id' : 0}))
        volumeMin = int(min(df['거래량평균%']))
        volumeMax = int(max(df['거래량평균%']))+10
        if volumeRange[1] == 0 :
            volumeRange = [volumeMin, volumeMax]
            
        tmp = filtered_rows(df.to_dict(orient='records'), volumeRange, reserveRatio, ratioRange, volumeAvg, marketCap, debtRatio, 'desc', '종목 등락률', ABC)
        tmp_df = pd.DataFrame(tmp)
        
        tableM1M2 = tmp_df[['M1', 'M2', '업종명', '종목명', '종목 등락률', '전일대비거래량', '종목코드', '테마명']].copy()
        tableM1M2['id'] = tableM1M2.index
        tableM1M2['인기'] = ''
        
        result = {
            'volumeMin' : volumeMin,
            'volumeMax' : volumeMax,
            # 'volumeRange' : volumeRange,
            'tableM1M2' : tableM1M2.to_dict(orient='records'),
            'tableM1M2Themes' : 상위10개테마추출(tmp_df),
            'industryTop10' : 상위10개테마추출(filtered_df),
        }
        return result 
    
    except Exception as e:
        return {"error" : str(e)}
    
@router.get('/findThemeStocks')
async def FindThemeStocks( name : str ):
    try :
        col = client.Info.ThemeStocks
        테마_df = pd.DataFrame(col.find({"테마명" : name},{'_id' : 0}))[['종목명', '종목코드']]

        col = client.Info.StockPriceDaily
        가격_df = pd.DataFrame(col.find({},{'_id' : 0, '종목코드':1,'업종명':1,'등락률':1,'전일대비거래량':1}))
        # tmp = 테마_df[테마_df['테마명'] == name][['종목명', '종목코드']]
        merge = pd.merge(테마_df, 가격_df, on='종목코드', how='left')
        result = merge.sort_values(by=['등락률'], ascending=False)
        result['id'] = [i for i in range(len(result.index))]
        
        return result.to_dict(orient='records')
    
    except Exception as e:
        return {"error" : str(e)}

@router.get('/findIndustryStocks')
async def FindIndustryStocks( name : str ):
    try :
        col = client.Info.IndustryStocks
        업종_df = pd.DataFrame(col.find({'업종명' : name},{'_id' : 0}))[['종목명', '종목코드']]

        col = client.Info.StockPriceDaily
        가격_df = pd.DataFrame(col.find({},{'_id' : 0, '종목코드':1,'업종명':1,'등락률':1,'전일대비거래량':1}))

        # tmp = 업종_df[업종_df['업종명'] == name][['종목명', '종목코드']]
        merge = pd.merge(업종_df, 가격_df, on='종목코드', how='left')
        result = merge.sort_values(by=['등락률'], ascending=False)
        result['id'] = [i for i in range(len(result.index))]
        
        return result.to_dict(orient='records')
    
    except Exception as e:
        return {"error" : str(e)}
