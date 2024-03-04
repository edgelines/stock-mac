from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta, time as dt_time
import pymongo
import pandas as pd
import numpy as np
import logging
from functools import reduce
import Api.findDay as find
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

# 영업일 확인 함수
def is_business_day(date, holiday_list):
    """주어진 datetime 객체가 영업일인지 확인합니다."""
    if date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holiday_list:
        return True
    return False

# 직전 영업일 찾기 함수
def find_previous_business_day(date, holiday_list):
    while not is_business_day(date, holiday_list):
        date -= timedelta(days=1)
    return date


class HTS:
    def __init__(self):
        collection = client['Info']['Holiday']
        휴일 = pd.DataFrame(collection.find({}, {'_id' : 0}))
        self.holiday_list = 휴일['휴일'].to_list()
        col = client.Info.StockEtcInfo
        StockEtcInfo = pd.DataFrame(col.find({}, {'_id' : 0, '종목코드':1,'시가총액':1}))
        col = client.Info.StockPriceDaily
        StockPriceDaily = pd.DataFrame(col.find({}, {'_id' : 0, '종목코드':1,'업종명':1,'등락률':1,'전일대비거래량':1}))
        col = client.Info.StockThemes
        StockThemes= pd.DataFrame(col.find({}, {'_id' : 0, '종목코드':1,'테마명':1}))
        col = client.Info.Financial
        StockFinance= pd.DataFrame(col.find({ "유보율": { '$gt': 100 } }, {'_id' : 0, '종목코드':1,'유보율':1}))
        col = client.HTS.CountTrading
        self.CountTrading = pd.DataFrame(col.find({},{'_id':0}))
        base = StockEtcInfo.merge(StockPriceDaily, on='종목코드').merge(StockThemes, on='종목코드').merge(StockFinance, on='종목코드')
        base['전일대비거래량'] =  round(base['전일대비거래량'] *100, 1)
        base['시가총액'] = round(base['시가총액']/100000000)
        base['대비율'] = base['등락률']
        # 소숫점 1자리 고정
        round_col = ['대비율', '등락률']
        for col in round_col :
            base[col] = round(base[col],1)
        
        # int 변환
        int_col = ['시가총액', '전일대비거래량']
        for col in int_col :
            base[col] = base[col].astype(int)

        self.base = base

    def get_holiday_list(self):
        return self.holiday_list

    def get_financial_list(self):
            col = client.HTS.Financial
            financial= list(col.find({ }, {'_id' : 0}))
            return financial
    
    def get_date(self, date):
        start_time = datetime.now()
        current_time = start_time.time()
        # print('HTS>get_date :', current_time)
        yyyy = date[0:4]
        mm = date[5:7]
        dd = date[8:10]

        tmp_date = datetime(int(yyyy), int(mm), int(dd), 0, 0, 0, 0)
        if is_business_day(tmp_date, self.holiday_list) & (current_time > dt_time(9,40)) :
            return date
        else :
            tmp_date = datetime(int(yyyy), int(mm), int(dd), 0, 0, 0, 0) - timedelta(1)
            yesterday = find_previous_business_day(tmp_date, self.holiday_list)
            # print('Yesterday :', yesterday)
            return yesterday.strftime('%Y-%m-%d')
    
    def get_dataframe(self, name, query, columnsName):
        col = client.HTS[name]
        data = list(col.find(query,{'_id' :0}))[-1]
        df = pd.DataFrame(data['Data'])
        df.rename(columns={'순매수대금' : columnsName}, inplace=True)
        return df
    
    def get_fixed_data_기관계(self, query, name ):
        name = f'Fixed{name}'
        기관계 = self.get_dataframe(f'{name}2', query, '기관계')
        보험 = self.get_dataframe(f'{name}3', query, '보험')
        금투 = self.get_dataframe(f'{name}5', query, '금투')
        투신 = self.get_dataframe(f'{name}4', query, '투신')
        연기금 = self.get_dataframe(f'{name}6', query, '연기금')
        기타법인 = self.get_dataframe(f'{name}7', query, '기타법인')
        
        merge = reduce(lambda x,y: pd.merge(x,y, on=['종목코드', '종목명'], how='outer'), [기관계,보험,금투,투신,연기금,기타법인])
        merge = merge.fillna(0)
        merge['보험기타금융'] = merge['보험'] + merge['금투']
        merge.drop(labels=['보험', '금투'], axis=1, inplace=True)
        merge = merge[(merge['기관계'] > 0) | (merge['투신'] > 0) | (merge['보험기타금융'] > 0) |(merge['연기금'] > 0) |(merge['기타법인'] > 0) ]
        df = pd.merge(self.base, merge, on='종목코드', how='inner')
        df['id'] = df.index
        return df

    def create_query(self, date_str, time_str=None):
        if time_str:
            # 시간 문자열이 제공되면, 날짜와 시간을 결합하여 datetime 객체를 생성합니다.
            datetime_obj = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            query = {'날짜': datetime_obj}
        else:
            # 시간 문자열이 제공되지 않으면, 그 날짜의 전체 범위를 대상으로 쿼리합니다.
            start_of_day = datetime.strptime(self.get_date(date_str), "%Y-%m-%d")
            end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)
            query = {'날짜': {'$gte': start_of_day, '$lt': end_of_day}}
        return query
    
    def get_data(self, query, name, columnsName ) :
        col = client.HTS[name]
        data = list(col.find(query,{'_id' :0}))[-1]
        df = pd.DataFrame(data['Data'])
        
        if 'Fixed' in name : 
            df.rename(columns={'순매수대금' : columnsName}, inplace=True)
            base = self.base
        else :
            base = self.base.drop(labels='대비율', axis=1)
        
        merged_df = df.merge(base, on='종목코드')

        round_col = ['대비율', '등락률']
        for col in round_col :
            merged_df[col] = round(merged_df[col],1)
        merged_df = merged_df[merged_df[columnsName] > 0].sort_values(by=columnsName, ascending=False)
        merged_df['id'] = merged_df.index
        return merged_df
    
    def preprocessing(self, name, date, time ):
        start_time = datetime.now()
        current_time = start_time.time()
        
        query ={}
        if date :
            query = self.create_query(date, time)
        
        if ((current_time >= dt_time(18,30)) | (current_time < dt_time(9,30) ) ) & ( time == 'null' ) :
            tmp1 = self.get_data(query, f'Fixed{name}1', '외국인')
            tmp2 = self.get_data(query, f'Fixed{name}2', '기관계')
            tmp9 = self.get_fixed_data_기관계(query ,name)
            tmp8 = pd.merge(tmp1, tmp2[['종목코드', '기관계']], on='종목코드', how='inner').sort_values(by=['외국인', '기관계'], ascending=False)
            tmp8 = tmp8.fillna(0)
        else :
            tmp1 = self.get_data(query, f'{name}1', '외국인')
            tmp2 = self.get_data(query, f'{name}2', '기관계')
            tmp3 = self.get_data(query, f'{name}3', '보험기타금융')
            tmp4 = self.get_data(query, f'{name}4', '투신')
            tmp6 = self.get_data(query, f'{name}6', '연기금')
            tmp7 = self.get_data(query, f'{name}7', '기타법인')
            
            tmp8 = pd.concat([tmp1, tmp2]).drop_duplicates(subset='종목코드', keep='first').sort_values(by=['외국인', '기관계'], ascending=False)
            tmp9 = pd.concat([tmp8, tmp3, tmp4, tmp6, tmp7]).drop_duplicates(subset='종목코드', keep='first').sort_values(by=['기관계', '투신', '연기금'], ascending=False)
        
        df1 = tmp1[['id', '종목코드', '종목명', '업종명', '테마명', '시가총액', '대비율', '등락률', '전일대비거래량', '외국인']]
        df2 = tmp9[['id', '종목코드', '종목명', '업종명', '테마명', '시가총액', '대비율', '등락률', '전일대비거래량', '기관계', '투신', '보험기타금융', '연기금', '기타법인']]
        df3 = tmp8[['id', '종목코드', '종목명', '업종명', '테마명', '시가총액', '대비율', '등락률', '전일대비거래량', '외국인', '기관계']]
        df3 = df3[(df3['외국인'] >0) & (df3['기관계'] >0)]
        
        df1 = df1.merge(self.CountTrading, on='종목코드')
        df2 = df2.merge(self.CountTrading, on='종목코드')

        df1 = df1.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)
        df1['id'] = df1.index
        df2['id'] = df2.index

        df1.rename(columns={'외국매수' : '연속거래일'}, inplace=True)
        df2.rename(columns={'기관매수' : '연속거래일'}, inplace=True)

        if len(df3) > 0 :
            df3 = df3.merge(self.CountTrading, on='종목코드')
            df3 = df3.reset_index(drop=True)
            df3['id'] = df3.index
            df3.rename(columns={'외기합산' : '연속거래일'}, inplace=True)
        
        조회 = datetime.now().date()
        조회일 = datetime(조회.year, 조회.month, 조회.day, 0,0,0)

        if is_business_day(조회일, self.holiday_list) :
            df1['연속거래일'] = df1['연속거래일'] +1
            df2['연속거래일'] = df2['연속거래일'] +1
            if len(df3) > 0 :
                df3['연속거래일'] = df3['연속거래일'] +1
                
        return {'df1' : df1, 'df2' : df2, 'df3' : df3}


def countIndustry(*dfs):
    total_counts = pd.Series(dtype=int)
    for df in dfs :
        if not df.empty :
            df_grouped = df.groupby('업종명').size()
            total_counts = total_counts.add(df_grouped, fill_value=0)

    df_total_counts = pd.DataFrame(total_counts, columns=['갯수'])
    df_total_counts.index.name = '업종명'
    df_total_counts.reset_index(inplace=True)
    df_total_counts.sort_values(by='갯수', inplace=True, ascending=False)
    return df_total_counts.to_dict(orient='records')

def countThemes(*dataframes):
    # 모든 테마명을 카운트하기 위한 Series를 빈 Series로 초기화합니다.
    theme_counts = pd.Series(dtype=int)
    
    # 각 데이터 프레임에 대해서 반복합니다.
    for df in dataframes:
        # 현재 데이터 프레임의 테마명을 하나의 리스트로 합칩니다.
        themes = sum(df['테마명'].tolist(), [])
        # 현재 데이터 프레임의 테마명을 카운트합니다.
        current_counts = pd.Series(themes).value_counts()
        # 기존의 카운트와 더합니다. fill_value=0은 없는 테마명은 0으로 취급하여 더합니다.
        theme_counts = theme_counts.add(current_counts, fill_value=0)
    
    # 결과를 데이터 프레임으로 변환합니다.
    theme_counts_df = theme_counts.astype(int).reset_index()
    theme_counts_df.columns = ['테마명', '갯수']
    theme_counts_df.sort_values(by='갯수', ascending=False, inplace=True)
    return theme_counts_df.to_dict(orient='records')

def statistics(df):
    if len(df) > 0 :
        per = int(len(df[df['등락률']>0]) / len(df) * 100)
        avg = round(df[df['등락률']>0]['등락률'].mean(),1)
        if np.isnan(avg) : avg = 0
        return {'up' : len(df[df['등락률']>0]), 'total' : len(df), 'per' : per, 'avg' : avg}
    else : 
        return {'up' : 0, 'total' : 0, 'per' : 0, 'avg' : 0}

def getConsecutiveTradingDay(*dfs):
    # 연속거래일 최대/최소
    total_counts = []
    for df in dfs :
        if not df.empty :
            max = int(df['연속거래일'].max())
            min = int(df['연속거래일'].min())
            total_counts.append({ 'max' : max, 'min' : min})
        else : 
            total_counts.append({ 'max' : 0, 'min' : 0})
    return total_counts

def find_split(df, find_col, ThemesName):
    filtered_df = df[df[find_col].apply(lambda x: ThemesName in x)]
    filtered_df.reset_index(inplace=True, drop=True)
    filtered_df['id']=filtered_df.index
    return filtered_df

# 하위 업종 > 선택시 POST
@router.post('/findData', response_class=JSONResponse)
async def FindData(req : Request):
    try :
        phoenix = HTS()
        holiday_list = phoenix.get_holiday_list()
        
        req_data = await req.json()
        typ = req_data['type']
        split = req_data['split']
        name = req_data['name']
        market = req_data['market']
        
        date = datetime.strptime(req_data['date'], '%Y-%m-%d')
        date = find_previous_business_day(date, holiday_list)
        # print( datetime.today().strftime('%y-%m-%d %H:%M:%S'), date, typ, split, name, market)
        
        if req_data['time'] == 'null' :
            time = None
        else : time = req_data['time']
        
        data = phoenix.preprocessing(market, date.strftime('%Y-%m-%d'), time)
        
        if split == '하위업종' :
            if typ == 'null':
                df1 = data['df1'][data['df1']['외국인'] >300]
                df2 = data['df2'][(data['df2']['기관계'] > 300) | (data['df2']['투신'] > 300) | (data['df2']['보험기타금융'] > 300) |(data['df2']['연기금'] > 300) |(data['df2']['기타법인'] > 300) ]
                df3 = data['df3'][(data['df3']['외국인'] > 300) & (data['df3']['기관계'] > 300) ]
                
                result = client['Industry']['LowRankTableTop3']
                data = pd.DataFrame(result.find({}, {'_id':0}).sort([('날짜', -1)]).limit(3))
                data=data.sort_values(by='날짜')
                find_list=[]
                for row in data['data']:
                    for item in row :
                        find_list.append(item['중복_업종명'])
                df1 = df1[df1['업종명'].isin(find_list)]
                df2 = df2[df2['업종명'].isin(find_list)]
                df3 = df3[df3['업종명'].isin(find_list)]
                try :
                    df1 = df1.sort_values(by=['연속거래일', '외국인'], ascending=[True, False])
                    df2 = df2.sort_values(by=['연속거래일', '기관계', '투신', '연기금'], ascending=[True, False, False, False])
                    df3 = df3.sort_values(by=['연속거래일', '외국인', '기관계'], ascending=[True, False, False])
                except Exception as e:
                    pass
                    # print('sort_value > ', e)

                if len(df3) > 0 :
                    result = {
                    'df1' : df1.to_dict(orient='records'),
                    'df2' : df2.to_dict(orient='records'),
                    'df3' : df3.to_dict(orient='records'),
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                    'consecutive': getConsecutiveTradingDay(df1, df2, df3)
                    }
                    
                else : 
                    result = {
                    'df1' : df1.to_dict(orient='records'),
                    'df2' : df2.to_dict(orient='records'),
                    'df3' : [],
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                    'consecutive': getConsecutiveTradingDay(df1, df2, df3)
                    }

            else :
                df1 = find_split(data['df1'], typ, name)
                df2 = find_split(data['df2'], typ, name)
                df3 = find_split(data['df3'], typ, name)
                            
                result = { 'df1' : df1.to_dict(orient='records'), 'df2' : df2.to_dict(orient='records'), 'df3' : df3.to_dict(orient='records'),
                        'industry' : countIndustry(df1, df2, df3),
                        'themes' : countThemes(df1, df2, df3),
                        'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                        'consecutive': getConsecutiveTradingDay(df1, df2, df3)
                        }
        
        elif split == '추정매매동향' :
            df1 = data['df1'][data['df1']['외국인'] >300]
            df2 = data['df2'][(data['df2']['기관계'] > 300) | (data['df2']['투신'] > 300) | (data['df2']['보험기타금융'] > 300) |(data['df2']['연기금'] > 300) |(data['df2']['기타법인'] > 300) ]
            df3 = data['df3'][(data['df3']['외국인'] > 300) & (data['df3']['기관계'] > 300) ]
            try :
                df11 = data['df1'].sort_values(by=['연속거래일', '외국인'], ascending=[True, False])
                df22 = data['df2'].sort_values(by=['연속거래일', '기관계', '투신', '연기금'], ascending=[True, False, False, False])
                df33 = data['df3'].sort_values(by=['연속거래일', '외국인', '기관계'], ascending=[True, False, False])
            except Exception as e:
                pass
                # print('sort_value > ', e)
                
            if typ == 'null' :
                if len(df3) > 0 :
                    result = {
                    'df1' : df11.to_dict(orient='records'),
                    'df2' : df22.to_dict(orient='records'),
                    'df3' : df33.to_dict(orient='records'),
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(data['df1']), statistics(data['df2']), statistics(data['df3'])],
                    'consecutive': getConsecutiveTradingDay(data['df1'], data['df2'], data['df3'])
                    }
                else : 
                    result = {
                    'df1' : df11.to_dict(orient='records'),
                    'df2' : df22.to_dict(orient='records'),
                    'df3' : [],
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(data['df1']), statistics(data['df2']), statistics(data['df3'])],
                    'consecutive': getConsecutiveTradingDay(data['df1'], data['df2'], data['df3'])
                    }


            else :
                tmp = pd.concat([df1, df2, df3]).drop_duplicates(subset='종목코드', keep='first')
                if typ == '업종명' :
                    cols = ['id','종목코드', '종목명', '업종명', '등락률', '전일대비거래량']
                else :
                    cols = ['id','종목코드', '종목명', '업종명', '테마명', '등락률', '전일대비거래량']
                filtered = find_split(tmp, typ, name)[cols]
                df1 = find_split(data['df1'], typ, name)
                df2 = find_split(data['df2'], typ, name)
                df3 = find_split(data['df3'], typ, name)
                            
                result = { 'df1' : df1.to_dict(orient='records'), 'df2' : df2.to_dict(orient='records'), 'df3' : df3.to_dict(orient='records'),
                        'industry' : countIndustry(df1, df2, df3),
                        'themes' : countThemes(df1, df2, df3),
                        'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                        'consecutive': getConsecutiveTradingDay(df1, df2, df3),
                        'filtered' : filtered.to_dict(orient='records')
                        }
        
        elif (split == '매출') | (split == '영업이익') | (split == '당기순이익') :
            financial = phoenix.get_financial_list()
            if split == '매출' :
                financial_col = '매출'
            elif split == '영업이익' :
                financial_col = '영업이익'
            elif split == '당기순이익' :
                financial_col = '당기순이익'
            
            q_code_list = financial[0][f'분기{financial_col}']
            a_code_list = financial[0][f'연간{financial_col}']
            분기_df1 = data['df1'][data['df1']['종목코드'].isin(q_code_list)]
            분기_df2 = data['df2'][data['df2']['종목코드'].isin(q_code_list)]
            분기_df3 = data['df3'][data['df3']['종목코드'].isin(q_code_list)]
            
            연간_df1 = data['df1'][data['df1']['종목코드'].isin(a_code_list)]
            연간_df2 = data['df2'][data['df2']['종목코드'].isin(a_code_list)]
            연간_df3 = data['df3'][data['df3']['종목코드'].isin(a_code_list)]
            
            df1 = pd.concat([분기_df1, 연간_df1])
            df2 = pd.concat([분기_df2, 연간_df2])
            df3 = pd.concat([분기_df3, 연간_df3])
            try :
                df1 = df1.drop_duplicates(subset='종목코드', keep='first').sort_values(by=['연속거래일', '외국인'], ascending=[True, False])
                df2 = df2.drop_duplicates(subset='종목코드', keep='first').sort_values(by=['연속거래일', '기관계', '투신', '연기금'], ascending=[True, False, False, False])
                df3 = df3.drop_duplicates(subset='종목코드', keep='first').sort_values(by=['연속거래일', '외국인', '기관계'], ascending=[True, False, False])
            except Exception as e:
                pass
                # print('sort_value > ', e)
            
            if typ == 'null' :
                df1['id'] = df1.index
                df2['id'] = df2.index
                df3['id'] = df3.index
                if len(df3) > 0 :
                    result = {
                    'df1' : df1.to_dict(orient='records'),
                    'df2' : df2.to_dict(orient='records'),
                    'df3' : df3.to_dict(orient='records'),
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                    'consecutive': getConsecutiveTradingDay(df1, df2, df3)
                    }
                else : 
                    result = {
                    'df1' : df1.to_dict(orient='records'),
                    'df2' : df2.to_dict(orient='records'),
                    'df3' : [],
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                    'consecutive': getConsecutiveTradingDay(df1, df2, df3)
                    }
            else :
                tmp = pd.concat([df1, df2, df3]).drop_duplicates(subset='종목코드', keep='first')
                if typ == '업종명' :
                    cols = ['id','종목코드', '종목명', '업종명', '등락률', '전일대비거래량']
                else :
                    cols = ['id','종목코드', '종목명', '업종명', '테마명', '등락률', '전일대비거래량']
                filtered = find_split(tmp, typ, name)[cols]
                df1 = find_split(data['df1'], typ, name)
                df2 = find_split(data['df2'], typ, name)
                df3 = find_split(data['df3'], typ, name)
                            
                result = { 'df1' : df1.to_dict(orient='records'), 'df2' : df2.to_dict(orient='records'), 'df3' : df3.to_dict(orient='records'),
                        'industry' : countIndustry(df1, df2, df3),
                        'themes' : countThemes(df1, df2, df3),
                        'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                        'consecutive': getConsecutiveTradingDay(df1, df2, df3),
                        'filtered' : filtered.to_dict(orient='records')
                        }

        # 연간(잠정)실적
        else :
            financial = phoenix.get_financial_list()
            
            code_list = financial[0]['추정']
            
            df1 = data['df1'][data['df1']['종목코드'].isin(code_list)]
            df2 = data['df2'][data['df2']['종목코드'].isin(code_list)]
            df3 = data['df3'][data['df3']['종목코드'].isin(code_list)]
            try :
                df1 = df1.sort_values(by=['연속거래일', '외국인'], ascending=[True, False])
                df2 = df2.sort_values(by=['연속거래일', '기관계', '투신', '연기금'], ascending=[True, False, False, False])
                df3 = df3.sort_values(by=['연속거래일', '외국인', '기관계'], ascending=[True, False, False])
            except Exception as e:
                pass
                # print('sort_value > ', e)
            if typ == 'null' :
                df1['id'] = df1.index
                df2['id'] = df2.index
                df3['id'] = df3.index
                if len(df3) > 0 :
                    result = {
                    'df1' : df1.to_dict(orient='records'),
                    'df2' : df2.to_dict(orient='records'),
                    'df3' : df3.to_dict(orient='records'),
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                    'consecutive': getConsecutiveTradingDay(df1, df2, df3)
                    }
                else : 
                    result = {
                    'df1' : df1.to_dict(orient='records'),
                    'df2' : df2.to_dict(orient='records'),
                    'df3' : [],
                    'industry' : countIndustry(df1, df2, df3),
                    'themes' : countThemes(df1, df2, df3),
                    'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                    'consecutive': getConsecutiveTradingDay(df1, df2, df3)
                    }
            
            else :
                tmp = pd.concat([df1, df2, df3]).drop_duplicates(subset='종목코드', keep='first')
                if typ == '업종명' :
                    cols = ['id','종목코드', '종목명', '업종명', '등락률', '전일대비거래량']
                else :
                    cols = ['id','종목코드', '종목명', '업종명', '테마명', '등락률', '전일대비거래량']
                filtered = find_split(tmp, typ, name)[cols]
                df1 = find_split(data['df1'], typ, name)
                df2 = find_split(data['df2'], typ, name)
                df3 = find_split(data['df3'], typ, name)
                            
                result = { 'df1' : df1.to_dict(orient='records'), 'df2' : df2.to_dict(orient='records'), 'df3' : df3.to_dict(orient='records'),
                        'industry' : countIndustry(df1, df2, df3),
                        'themes' : countThemes(df1, df2, df3),
                        'statistics' : [statistics(df1), statistics(df2), statistics(df3)],
                        'consecutive': getConsecutiveTradingDay(df1, df2, df3),
                        'filtered' : filtered.to_dict(orient='records')
                        }
            
        return result        
    except Exception as e:
        logging.error('post(/findData)', e)
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})





# @router.get('/{name}')
# async def loadDB(name):
#     try :
#         result = client['Etc'][name.title()]
#         return list(result.find({}, {'_id':False}))
#     except Exception as e:
#         logging.error(e)
#         return JSONResponse(status_code=500, content={"message": "Internal Server Error"})