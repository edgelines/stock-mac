from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import pymongo, json, redis
from functools import reduce
import talib.abstract as ta
# from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

def process_dataframe(df, column_name, separator_name):
    df = df.drop_duplicates(column_name, keep='first').reset_index(drop=True)
    if column_name == '업종명' or column_name == '종목명' :
        df = df[df['업종명'] != '기타']
    df['구분'] = separator_name
    df = df[[column_name, '구분']]
    df.columns = ['search', 'separator']
    return df

@router.get('/stockSearchInfo')
async def StockSearchInfo():
    try :
        db = client['Info']
        col업종 = db['IndustryStocks']
        col테마 = db['ThemeStocks']
        
        tmp1 = pd.DataFrame(col업종.find({},{'_id' :0}))
        tmp2 = pd.DataFrame(col테마.find({},{'_id' :0}))
        
        themesDF = process_dataframe(tmp2, '테마명', '테마')
        sectorsDF = process_dataframe(tmp1, '업종명', '업종')
        stocksDF = process_dataframe(tmp1, '종목명', '종목')
        data = pd.concat([themesDF, sectorsDF, stocksDF], axis=0).reset_index(drop=True)
        return data.to_dict('records')
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.get('/treasury')
async def Treasury():
    try :
        col_현재가 = client.Info.StockPriceDaily
        주식 = pd.DataFrame(col_현재가.find({},{'_id' : 0, '종목명' :0, '거래량' :0, '전일거래량' :0, '시가' :0, '고가' :0, '저가' :0, '전일대비거래량' : 0}))
        col_자기주식 = client.AoX.TreasuryStock
        자기주식 = pd.DataFrame(col_자기주식.find({},{'_id' : 0}))
        col_기타 = client['Info']['StockEtcInfo']
        기타 = pd.DataFrame(col_기타.find({},{'_id' : 0, '종목코드':1, '시장' :1, 'PBR' :1}))
        col_재무 = client.Info.StockFinance
        재무 = pd.DataFrame(col_재무.find({},{'_id' : 0, '종목명' :0, '시가총액' :0}))
        
        # col_재무 = client.Info.Financial
        # 재무 = pd.DataFrame(col_재무.find({},{'_id' : 0, '분기실적' :0, '연간실적' :0}))
        
        중간정리 = pd.merge(자기주식, 주식, on='종목코드', how='left')
        중간정리.dropna(inplace=True)
        중간정리['수익률'] = 중간정리.apply(lambda x: ((x['현재가'] - x['평균단가']) / x['평균단가'])*100 if x['평균단가'] != 0 else 0, axis=1)
        # 중간정리['총액'] = 중간정리['총액'] / 1000000 #총액 100만원단위
        
        중간정리 = pd.merge(중간정리, 기타, on='종목코드', how='left')
        중간정리 = pd.merge(중간정리, 재무, on='종목코드', how='left')
        
        중간정리 = 중간정리[(중간정리['유보율'] > 200) & (중간정리['부채비율'] < 400) & (중간정리['유보율'] > 중간정리['부채비율'])]

        중간정리['id'] = 중간정리.index
        중간정리 = 중간정리.fillna(0)
        columns_to_convert = ['평균단가', '수익률', 'PBR']
        for col in columns_to_convert:
            중간정리[col] = round(중간정리[col],1)

        return 중간정리.to_dict('records')
    
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

    
class SearchFinancial:
    def __init__(self):    
        # 업종정보
        col = client.Info.IndustryStocks
        self.Industry = pd.DataFrame(col.find({},{'_id':0}))
        
        # 분기별 매출/영업이익/순이익 증감수
        col = client.HTS.Financial
        self.data = list(col.find({},{'_id' : 0}))[0]
        
        # 동일업종PER
        col = client.Info.CompanyOverview
        self.CompanyOverview = pd.DataFrame(col.find({},{'_id':0, '종목코드':1, '동일업종PER' :1}))
        
        # PER, PBR
        col = client.Info.StockEtcInfo
        self.StockEtcInfo = pd.DataFrame(col.find({},{'_id':0, '종목코드':1, 'PER' :1, 'PBR' :1 }))
        
        # 종목별 재무제표 + 유보율, 부채비율
        col = client.Info.Financial
        self.Financial = pd.DataFrame(col.find({},{'_id':0, '종목코드':1, '유보율':1, '부채비율' :1}))
        
        # 당일 데이터
        col = client.Info.StockPriceDaily
        StockPriceDaily= pd.DataFrame(col.find({},{'_id':0, '종목코드':1, '시가' :1, '고가' :1, '저가':1, '현재가':1}))
        StockPriceDaily.columns = ['종목코드', '시가', '고가', '저가', '종가']
        self.StockPriceDaily = StockPriceDaily
        
        # 이벤트
        col = client.Schedule.StockEvent
        self.StockEvent = list(col.find({},{'_id':0}))
        
        # 업종순위
        col = client.Industry.Rank
        self.IndustryRank = pd.DataFrame(col.find({}, {'_id':0, '업종명':1, '전일대비':1, '순위':1}))

        # 코스피/코스닥
        col = client.Info.MarketList
        self.MarketList = list(col.find({},{'_id':0}))[0]
        
    def willR(self, stock_code):
        
        redis_client = redis.Redis(host='192.168.0.3', port=6379, db=0)
        data_json = redis_client.get(stock_code).decode('utf-8')
        df = pd.read_json(data_json)
        
        stock = self.StockPriceDaily 
        add = stock[stock['종목코드'] == stock_code]
        add['날짜'] = datetime.today().strftime('%Y-%m-%d')
        
        df = pd.concat([df, add[['날짜', '시가', '고가', '저가', '종가']]])
        # df['날짜'] = pd.to_datetime(df['날짜'])
        df.reset_index(drop=True, inplace=True)
        
        WillR9 = ta.WILLR(df['고가'], df['저가'], df['종가'], timeperiod=9)
        WillR14 = ta.WILLR(df['고가'], df['저가'], df['종가'], timeperiod=14)
        WillR33 = ta.WILLR(df['고가'], df['저가'], df['종가'], timeperiod=33)
        
        return {
            'WillR9' : WillR9,
            'WillR14' : WillR14,
            'WillR33' : WillR33,
        } 
    
    def find_events_for_stock(self, stock_name):
        """종목명에 해당하는 이벤트를 찾아서 '이벤트' 컬럼에 추가하기 위한 함수 정의

        Args:
            stock_name (_type_): _description_

        Returns:
            _type_: _description_
        """
        # 종목명에 해당하는 이벤트를 필터링
        filtered_events = [event for event in self.StockEvent if event['item'] == stock_name]
        # 필터링된 이벤트가 있으면 이벤트 정보를 문자열로 반환, 없으면 빈 문자열 반환
        if filtered_events:
            return ', '.join([event['event'] for event in filtered_events])
        else:
            return ''
    
    def find_market_name(self, code):
        if code in self.MarketList['코스피'] :
            return 'Kospi'
        else :
            return 'Kosdaq'
    
    def get_category_industry(self, target_category=None, target_industry=None):
        분기매출 = self.data['분기_매출']
        분기영업이익 = self.data['분기_영업이익']
        분기당기순이익 = self.data['분기_당기순이익']
        추정 = self.data['추정']
        전년동분기대비 = self.data['전년동분기대비']
        
        흑자_매출 = self.data['흑자_매출']
        흑자_영업이익 = self.data['흑자_영업이익']
        흑자_당기순이익 = self.data['흑자_당기순이익']
        전년_해당년도_매출 = self.data['가결산_매출']
        전년_해당년도_영업이익 = self.data['가결산_영업이익']
        전년_해당년도_당기순이익 = self.data['가결산_당기순이익']
        
        종목리스트 = []
        if target_category is None :
            종목리스트 += self.data['전체']
        else :
            for cate in target_category:
                if cate == '가결산_매출' :
                    종목리스트 += 전년_해당년도_매출
                elif cate == '가결산_영업이익' :
                    종목리스트 += 전년_해당년도_영업이익
                elif cate == '가결산_당기순이익' :
                    종목리스트 += 전년_해당년도_당기순이익
                elif cate == '흑자기업' or cate == '흑자' :
                    종목리스트 += 흑자_매출+흑자_영업이익+흑자_당기순이익
                elif cate == '집계' :
                    종목리스트 += 전년_해당년도_매출+전년_해당년도_영업이익+전년_해당년도_당기순이익
                elif cate == '분기' :
                    종목리스트 += 분기매출+분기영업이익+분기당기순이익
                elif cate in ['매출', '영업이익', '당기순이익'] :
                    pass
                elif cate == '미집계' :
                    종목리스트 += 추정
                elif cate == '전년동분기대비' :
                    종목리스트 += 전년동분기대비
                else : 
                    종목리스트 += self.data[cate]
        
        df_raw = self.Industry
        # if target_industry == '제약' :
        #     df_raw = df_raw[df_raw['업종명'] == target_industry]
        if target_industry != None :
            df_raw = df_raw[df_raw['업종명'].isin(target_industry)]
            # df_raw = df_raw[df_raw['업종명'] == target_industry]
        
        종목리스트 = list(set(종목리스트))
        df_raw = df_raw[df_raw['종목코드'].isin(종목리스트)]
        df_raw['이벤트'] = df_raw['종목명'].apply(self.find_events_for_stock)
        df_raw = df_raw.merge(self.StockEtcInfo, how='left', on='종목코드').merge(self.Financial, how='left', on='종목코드').merge(self.CompanyOverview, how='left', on='종목코드')
        df_raw['시장'] = df_raw['종목코드'].apply(self.find_market_name)
        
        return df_raw
    
    def get_category_industry_with_willR(self, target_category=None, target_industry=None):
        df_raw = self.get_category_industry(target_category, target_industry)
        for index, row in df_raw.iterrows():
            stock_code = row['종목코드']
            willR_values = self.willR(stock_code)
            
            # WillR 값들을 df에 추가
            try :
                df_raw.at[index, 'WillR9'] = round(willR_values['WillR9'][-1],1)
            except :
                df_raw.at[index, 'WillR9'] = ''
            try :
                df_raw.at[index, 'WillR14'] = round(willR_values['WillR14'][-1],1)
            except :
                df_raw.at[index, 'WillR14'] = ''
            try :
                df_raw.at[index, 'WillR33'] = round(willR_values['WillR33'][-1], 1)
            except : 
                df_raw.at[index, 'WillR33'] = ''
            
        return df_raw

def get_매출_영업이익_순이익_증감수(industry, stock_list, column_name):
    data = industry[industry['종목코드'].isin(stock_list)]
    data = data.groupby(by='업종명').count().reset_index().drop(columns='종목명', axis=1)
    data.columns=['업종명', column_name]
    return data

@router.get('/searchFinancial')
async def Search():
    try :
        base = SearchFinancial()
        업종_count = base.Industry.groupby(by='업종명').count().reset_index().drop(columns='종목명', axis=1)
        업종_count.columns=['업종명', '전체종목수']
        업종_count = 업종_count.merge(base.IndustryRank, on='업종명', how='left')
        흑자기업수 = list(set( base.data['흑자_매출'] + base.data['흑자_영업이익'] + base.data['흑자_당기순이익'] ))

        가결산_매출 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['가결산_매출'], '가결산_매출')
        가결산_영업이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['가결산_영업이익'], '가결산_영업이익')
        가결산_당기순이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['가결산_당기순이익'], '가결산_당기순이익')
        분기_매출 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['분기_매출'], '분기_매출')
        분기_영업이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['분기_영업이익'], '분기_영업이익')
        분기_당기순이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['분기_당기순이익'], '분기_당기순이익')
        
        흑자_매출 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['흑자_매출'], '흑자_매출')
        흑자_영업이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['흑자_영업이익'], '흑자_영업이익')
        흑자_당기순이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['흑자_당기순이익'], '흑자_당기순이익')
        흑자기업수 = get_매출_영업이익_순이익_증감수(base.Industry, 흑자기업수, '흑자기업')
        미집계 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['추정'], '미집계')
        미집계_매출 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['미집계_매출'], '미집계_매출')
        미집계_영업이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['미집계_영업이익'], '미집계_영업이익')
        미집계_당기순이익 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['미집계_당기순이익'], '미집계_당기순이익')
        전년동분기대비 = get_매출_영업이익_순이익_증감수(base.Industry, base.data['전년동분기대비'], '전년동분기대비')

        industry = base.Industry.drop(columns=['종목명', '종목코드'], axis=1).drop_duplicates(subset='업종명', keep='first').reset_index(drop=True)
        industry = industry[industry['업종명'] != '기타']

        dfs = [ industry,업종_count, 가결산_매출, 가결산_영업이익, 가결산_당기순이익, 분기_매출, 분기_영업이익, 분기_당기순이익, 흑자_매출, 흑자_영업이익, 흑자_당기순이익, 미집계, 미집계_매출,미집계_영업이익,미집계_당기순이익, 전년동분기대비, 흑자기업수 ]
        industry = reduce(lambda left, right: pd.merge(left, right, on='업종명', how='left'), dfs)

        industry = industry.fillna(0)
        industry['순이익기업'] = round(industry['흑자기업'] / industry['전체종목수'] * 100, 0)

        cols = ['가결산_매출', '가결산_영업이익', '가결산_당기순이익', '분기_매출', '분기_영업이익', '분기_당기순이익', '흑자_매출', '흑자_영업이익', '흑자_당기순이익', '미집계', '미집계_매출','미집계_영업이익','미집계_당기순이익', '전년동분기대비', '순이익기업', '흑자기업']
        for col in cols :
            industry[col] = industry[col].apply(pd.to_numeric, errors = 'coerce').fillna(0)
            industry[col] = industry[col].astype(int)

        industry = industry[industry['전체종목수'] > 1 ]
        industry = industry.reset_index(drop=True)
        industry.sort_values(by='전일대비', ascending=False, inplace=True)
        industry['id'] = industry.index

        return industry.to_dict('records')
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "SearchFinancial Server Error"})

def preprocessing_코스피_코스닥_업종갯수(df, 구분='Kospi', columns=['업종명', '전체종목수']):
    df = df[df['시장'] == 구분]
    df = df.groupby(by='업종명').count().reset_index().drop(columns=['종목명', '시장'], axis=1)
    df.columns=columns
    return df

@router.get('/searchMarket')
async def Search_market():
    try :
        base = SearchFinancial()
        
        업종_count = base.Industry.copy()
        업종_count['시장'] = 업종_count['종목코드'].apply(base.find_market_name)

        코스피_전체 = preprocessing_코스피_코스닥_업종갯수(업종_count, 'Kospi')
        코스닥_전체 = preprocessing_코스피_코스닥_업종갯수(업종_count, 'Kosdaq')

        list_흑자기업 = list(set(base.data['흑자_매출'] + base.data['흑자_영업이익'] + base.data['흑자_당기순이익'] ))
        df_흑자기업 = pd.DataFrame(list_흑자기업, columns=['종목코드'])
        df_흑자기업['시장'] = df_흑자기업['종목코드'].apply(base.find_market_name)
        df_흑자기업 = df_흑자기업.merge(base.Industry, on='종목코드', how='left')

        코스피_흑자기업 = preprocessing_코스피_코스닥_업종갯수(df_흑자기업, 'Kospi', ['업종명', '흑자기업'])
        코스닥_흑자기업 = preprocessing_코스피_코스닥_업종갯수(df_흑자기업, 'Kosdaq', ['업종명', '흑자기업'])

        코스피 = 코스피_흑자기업.merge(코스피_전체, on='업종명', how='left')
        코스닥 = 코스닥_흑자기업.merge(코스닥_전체, on='업종명', how='left')

        result = {
            'Kospi_data' : 코스피.to_dict(orient='records'),
            'Kospi_total' : int(코스피['전체종목수'].sum()),
            'Kospi_profitable' : int(코스피['흑자기업'].sum()),
            'Kosdaq_data' : 코스닥.to_dict(orient='records'),
            'Kosdaq_total' : int(코스닥['전체종목수'].sum()),
            'Kosdaq_profitable' : int(코스닥['흑자기업'].sum()),
        }
        # print(result)
        return result
    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "SearchFinancial Server Error"})
    
@router.post('/findData', response_class=JSONResponse)
async def FindData(req : Request):
    try :
        base = SearchFinancial()
        req_data = await req.json()
        target_category = req_data['target_category']
        target_industry = req_data['target_industry']
        WillR = req_data['WillR']
        market = req_data['market']

        if WillR == 'X' :
            get_data = base.get_category_industry(target_category=target_category, target_industry=target_industry)
        else : 
            try :
                get_data = base.get_category_industry_with_willR(target_category=target_category, target_industry=target_industry)
            except :
                get_data = base.get_category_industry(target_category=target_category, target_industry=target_industry)
        
        if market != None :
            get_data = get_data[get_data['시장'] == market]
        
        get_data = get_data.fillna(0)
        get_data['id'] = get_data.index
        # print(get_data, get_data.info(), get_data.to_dict(orient='records'))
        return get_data.to_dict(orient='records')

    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})

@router.post('/crossChart', response_class=JSONResponse)
async def FindData(req : Request):
    try :
        base = SearchFinancial()
        req_data = await req.json()
        집계 = req_data['aggregated']
        흑자 = req_data['surplus']

        cate_1 = req_data['target_category1']
        cate_2 = req_data['target_category2']
        target_industry = req_data['target_industry']
        
        col = client.Info.FinancialGrowth
        financial_growth = list(col.find({},{'_id':0}))[0]
                
        종목리스트, target_category=[], []
        if 집계 == None:
            # 전체 종목을 가져오는것.
            # for item1 in ['가결산', '미집계']:
            #     for item2 in cate_2:
            #         target_category.append(f'{item1}_{item2}')
            
            get_data = base.get_category_industry(target_category=None, target_industry=target_industry)
            종목리스트 = financial_growth['전체']
            
        elif 집계 :
            for item1 in cate_1:
                if item1 in ['가결산'] :
                    종목리스트 += financial_growth['가결산']
                            
                elif item1 in ['분기'] :
                    종목리스트 += financial_growth['분기']
                
                elif item1 in ['전년동분기대비'] :
                    종목리스트 += financial_growth['전년동분기대비']
                
                for item2 in cate_2:
                    target_category.append(f'{item1}_{item2}')
            get_data = base.get_category_industry(target_category=target_category, target_industry=target_industry)
            
            # for cate in cate_1:

            if 흑자 :
                get_check = base.get_category_industry(target_category=['흑자'], target_industry=target_industry)
                get_data = get_data[get_data['종목코드'].isin(get_check['종목코드'].to_list())]
                
            else : 
                종목리스트 = financial_growth['전체']
            

        # 미집계 일경우
        else :
            종목리스트 += financial_growth['미집계'] + financial_growth['전체']
            
            if 흑자 :
                for cate_name in cate_2 :
                    target_category.append(f'미집계_흑자_{cate_name}')
                get_data = base.get_category_industry(target_category=target_category, target_industry=target_industry)
            else :
                for cate_name in cate_2 :
                    target_category.append(f'미집계_{cate_name}')
                get_data = base.get_category_industry(target_category=target_category, target_industry=target_industry)
                
        
        stock_df = pd.DataFrame(종목리스트)
        stock_df = stock_df.drop_duplicates(subset='종목코드', keep='first')
        get_data = get_data.merge(stock_df, on='종목코드', how='left').dropna()
        get_data['id'] = get_data.index
        return get_data.to_dict(orient='records')

    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
    
@router.post('/findCrossData', response_class=JSONResponse)
async def FindData(req : Request):
    try :
        base = SearchFinancial()
        req_data = await req.json()
        집계 = req_data['aggregated']
        흑자 = req_data['surplus']

        cate_1 = req_data['target_category1']
        cate_2 = req_data['target_category2']
        target_industry = req_data['target_industry']
                        
        target_category=[], []
        # 집계 일경우
        if 집계 :
            for item1 in cate_1:
                for item2 in cate_2:
                    target_category.append(f'{item1}_{item2}')

        # 미집계 일경우
        else :
            if 흑자 :
                for cate_name in cate_2 :
                    target_category.append(f'미집계_흑자_{cate_name}')
            else : 
                for cate_name in cate_2 :
                    target_category.append(f'미집계_{cate_name}')
        
        try :
            if target_industry != None :
                get_data = base.get_category_industry_with_willR(target_category=target_category, target_industry=target_industry)
            else :
                get_data = base.get_category_industry(target_category=target_category, target_industry=target_industry)    
        except :
            get_data = base.get_category_industry(target_category=target_category, target_industry=target_industry)
                
        get_data = get_data.fillna(0)
        get_data['id'] = get_data.index
        return get_data.to_dict(orient='records')

    except Exception as e:
        logging.error(e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})