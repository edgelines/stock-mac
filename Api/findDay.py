import pandas as pd
import pymongo
from datetime import datetime, timedelta
client = pymongo.MongoClient(host=['192.168.0.3:27017'])

collection = client['Info']['Holiday']
휴일 = pd.DataFrame(collection.find({}, {'_id' : 0}))
holiday_list = 휴일['휴일'].to_list()

def 오늘이영업일인가(working_ymd):
    yyyy = working_ymd[0:4]
    mm = working_ymd[5:7]
    dd = working_ymd[8:10]
    tmp_date = datetime(int(yyyy), int(mm), int(dd), 0, 0, 0, 0)

    yyyy = str(tmp_date)[0:4]
    mm = str(tmp_date)[5:7]
    dd = str(tmp_date)[8:10]

    if(tmp_date.weekday() >= 5) or (yyyy + '-' + mm + '-' + dd in holiday_list):
        return False
    else : return True

def 영업일찾기(working_ymd) :
    yyyy = working_ymd[0:4]
    mm = working_ymd[5:7]
    dd = working_ymd[8:10]

    tmp_date = datetime(int(yyyy), int(mm), int(dd), 0, 0, 0, 0) - timedelta(1)

    while True:
        yyyy = str(tmp_date)[0:4]
        mm = str(tmp_date)[5:7]
        dd = str(tmp_date)[8:10]

        if(tmp_date.weekday() >= 5) or (yyyy + '-' + mm + '-' + dd in holiday_list):
            tmp_date = tmp_date - \
                timedelta(max(1, (tmp_date.weekday()+6) % 7 - 3))
        else:
            break

    return f'{yyyy}-{mm}-{dd}'
    # """주어진 날짜가 영업일인지 확인합니다."""
    # yyyy, mm, dd = working_ymd[:4], working_ymd[5:7], working_ymd[8:10]
    # day = datetime(int(yyyy), int(mm), int(dd))
    # if day.weekday() >= 5 or f"{yyyy}-{mm}-{dd}" in holiday_list:
    #     return False
    # return True