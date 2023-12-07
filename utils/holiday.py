import requests 
# import datetime 
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
load_dotenv()

def nth_weekday(the_date, nth_week, week_day):
    temp = the_date.replace(day=1)
    adj = (week_day - temp.weekday()) % 7
    temp += timedelta(days=adj)
    temp += timedelta(weeks=nth_week-1)
    return temp

def get_recent_due(mydate:datetime):
    thismonth_duedate = nth_weekday(mydate, 2, 3)
    if mydate <= thismonth_duedate:
        return thismonth_duedate
    elif mydate > thismonth_duedate :
        nextmonth_duedate = nth_weekday(mydate+relativedelta(months=1),2, 3)
        return nextmonth_duedate

def get_ksat_date(year) -> datetime:
    mydate = datetime(year,11,1)
    result = nth_weekday(mydate, 3, 3)
    return result

def get_first_weekday_of_year(year: int) -> datetime:
    first_day = datetime(year, 1, 2)
    while first_day.weekday() > 4:
        first_day += timedelta(days=1)
    return first_day

def run(): 
    with MongoClient('mongodb://localhost:27017/') as client:
        collection = client['Search']['Holiday']
        holiday = list(collection.find({}, {'_id':False}))
    return holiday
    # api_url = os.getenv("API_URL_HOLIDAY")
    # requestData = requests.get(api_url)
    # data = pd.DataFrame(requestData.json())
    # return data['휴일'].to_list()