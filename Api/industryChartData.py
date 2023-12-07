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
async def IndustryChartData( request:Request ):
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