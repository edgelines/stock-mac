import pandas as pd
import talib.abstract as ta

def 날짜전처리(df):
    df['날짜'] = pd.to_datetime(df['날짜']).astype('int64') // 10**6
    return df.to_dict(orient='split')['data']

# MA

def 저가지수(origin_df, num, 가격기준) :
    df = origin_df.copy()
    df[f'ema{num}'] = ta.EMA(df[가격기준], timeperiod=num)
    df = df.drop(labels=가격기준, axis=1)
    df = df.dropna()
    return 날짜전처리(df)