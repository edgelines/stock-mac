import pandas as pd
from pymongo import MongoClient

def cal_WillR(df, n_days_list=[14]):
    df = df.copy()
    results = {}
    try :
        for n_days in n_days_list:
            low_min = df['저가'].rolling(window=n_days, center=False).min()
            high_max = df['고가'].rolling(window=n_days, center=False).max()
            df['willr'] = (high_max - df['종가']) / ( high_max - low_min ) * -100
            results[n_days] = df['willr'][-1:].values[0]
    except :
        results = {n_days: '-' for n_days in n_days_list}
    return results
    
def cal_DMI(data, n_list=[5], method='단순'):
    data = data.reset_index()
    results = {}
    try :
        data['TR'] = 0.0
        data['DMplus'] = 0.0
        data['DMminus'] = 0.0
        for n in n_list:
            for i in range(1, len(data)):  # Start from the second row
                data.loc[i, 'TR'] = max(data.loc[i, '고가'] - data.loc[i, '저가'], abs(data.loc[i, '고가'] - data.loc[i-1, '종가']), abs(data.loc[i, '저가'] - data.loc[i-1, '종가']))
                if (data.loc[i, '고가'] - data.loc[i-1, '고가']) > (data.loc[i-1, '저가'] - data.loc[i, '저가']):
                    data.loc[i, 'DMplus'] = data.loc[i, '고가'] - data.loc[i-1, '고가']
                    data.loc[i, 'DMminus'] = 0.0
                else:
                    data.loc[i, 'DMplus'] = 0.0
                    data.loc[i, 'DMminus'] = data.loc[i-1, '저가'] - data.loc[i, '저가']

            if method == '단순' :
                data['TRn'] = data['TR'].rolling(n).sum()
                data['DMplusN'] = data['DMplus'].rolling(n).sum()
                data['DMminusN'] = data['DMminus'].rolling(n).sum()
            elif method == '가중' :
                # 가중치 설정
                weights = [i for i in range(1, n+1)]
                # WMA로 TR, DMplus, DMminus 계산
                data['TRn'] = data['TR'].rolling(window=n).apply(lambda x: weighted_moving_average(x, weights), raw=True)
                data['DMplusN'] = data['DMplus'].rolling(window=n).apply(lambda x: weighted_moving_average(x, weights), raw=True)
                data['DMminusN'] = data['DMminus'].rolling(window=n).apply(lambda x: weighted_moving_average(x, weights), raw=True)

            data['DIplusN'] = 100 * (data['DMplusN'] / data['TRn'])
            data['DIminusN'] = 100 * (data['DMminusN'] / data['TRn'])
            data['DIdiff'] = abs(data['DIplusN'] - data['DIminusN'])
            data['DIsum'] = data['DIplusN'] + data['DIminusN']
            data['DX'] = 100 * (data['DIdiff'] / data['DIsum'])

            ADX = [0.0]
            for i in range(1, len(data)):
                if i < 2*n-1:
                    ADX.append(0.0)
                elif i == 2*n-1:
                    ADX.append(data['DX'][i-n+1:i+1].mean())
                elif i > 2*n-1:
                    ADX.append(((n-1) * ADX[-1] + data['DX'][i]) / n)
            data['ADX'] = ADX
            results[n] = data['DIplusN'][-1:].values[0]
    except :
        results = {n : '-' for n in n_list}
    return results

# 가중이동평균
def weighted_moving_average(values, weights):
    return sum(val * weight for val, weight in zip(values, weights)) / sum(weights)

# def calculate_indicators(stock_data):
#     willR_values = cal_WillR(stock_data, n_days_list=[5, 7, 14, 20, 33])
#     DMI_values = cal_DMI(stock_data, n_list=[3, 4, 5], method='가중')
#     return willR_values, DMI_values

# indicator 계산 함수
def calculate_for_ticker(row):
    종목코드 = row['종목코드']
    with MongoClient('mongodb://localhost:27017/') as client:
        collection = client['Stock'][종목코드]
        stock_data = pd.DataFrame(collection.find({}, {'_id': False})) # 필요한 필드만 가져옴
        데이터 = stock_data.iloc[-40:].copy()
        DMI데이터 = stock_data.iloc[-10:].copy()
        try:
            # willR_values, DMI_values = calculate_indicators(데이터)
            willR_values = cal_WillR(데이터, n_days_list=[5, 7, 14, 20, 33])
            DMI_values = cal_DMI(DMI데이터, n_list=[3, 4, 5,6,7], method='가중')

            indicator_data = {
                "티커": row['종목코드'],
                "업종명": row['업종명'],
                "종목명": row['종목명'],
                "시가": row['시가'],
                "고가": row['고가'],
                "저가": row['저가'],
                "종가": row['현재가'],
                "거래량": row['거래량'],
                "willR_5": willR_values[5],
                "willR_7": willR_values[7],
                "willR_14": willR_values[14],
                "willR_20": willR_values[20],
                "willR_33": willR_values[33],
                "DMI_3": DMI_values[3],
                "DMI_4": DMI_values[4],
                "DMI_5": DMI_values[5],
                "DMI_6": DMI_values[6],
                "DMI_7": DMI_values[7],
            }
            return indicator_data
        except Exception as e:
            print(f"Error for ticker {종목코드}: {e}")
            return None