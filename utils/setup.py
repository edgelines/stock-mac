import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

def cal_DMI_1(df, n_list=[5], method='단순'):
    data = df.copy()
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
        df[f'DMI_{n}'] = data['DIplusN']
    return df

# 가중이동평균
def weighted_moving_average(values, weights):
    return sum(val * weight for val, weight in zip(values, weights)) / sum(weights)


with MongoClient('mongodb://localhost:27017/') as client:
    # 사용할 데이터베이스 선택
    db = client['Stock']
    collection_list = db.list_collection_names()

    for collection_name in tqdm(collection_list):
        collection = db[collection_name]
        
        df = pd.DataFrame(collection.find({}, {'_id':False}))
        n_list = [1, 2, 3, 4, 5, 6, 7]
        dmi_results = cal_DMI_1(df, n_list, method='가중')
        dmi_results = dmi_results.fillna('-')
        # 계산된 결과 MongoDB에 업데이트
        updates = []
        for index, row in dmi_results.iterrows():
            update_data = {f'DMI_{n}': row[f'DMI_{n}'] for n in n_list}
            updates.append(UpdateOne({'_id': row['_id']}, {'$set': update_data}))

        if updates:
            collection.bulk_write(updates)
        