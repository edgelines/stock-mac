from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
import pandas as pd


def insertDB (tableName, collectionName, data, updateKey ) :
    with MongoClient('mongodb://localhost:27017/') as client:
        collection = client[tableName][collectionName]
        operations = []
        # Bulk operation 준비
        # df = pd.DataFrame(data)
        # for idx, row in tqdm(df.iterrows()):
        #     op = UpdateOne({updateKey: row[updateKey]}, {"$set": row.to_dict()}, upsert=True)
        #     operations.append(op)

        for item in tqdm(data):
            op = UpdateOne({updateKey: item[updateKey]}, {"$set": item}, upsert=True)
            operations.append(op)
        # Bulk operation 실행
        if operations:
            collection.bulk_write(operations)