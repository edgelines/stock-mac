import requests 
# import datetime 
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
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
    api_url = os.getenv("API_URL_HOLIDAY")
    requestData = requests.get(api_url)
    data = pd.DataFrame(requestData.json())
    return data['휴일'].to_list() 
    # url = "https://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx" 
    # year = datetime.today().now().year # 휴장일 검색 연도 
    # data = {"search_bas_yy": year,
    #     "gridTp": "KRX", 
    #     "pagePath": "/contents/MKD/01/0110/01100305/MKD01100305.jsp", 
    #     "code": 'VwN0qWxNxoQd3GptLiFi7VpQSV4Ewa+d2Su7DXPyhf9QzGrcwc/rwEcTS38k4e2df5Yx0Mfnbi2PWDHmer4lQzKMoOk5t9O8/DabZgelyz9UBc82a6GP7G4MABRDdIaJ7T+v79W6ON5hsRRGRUrUj69+eqY/BlbgIhBGzjGwqsT+CtNJN0ckkY/7efqYEaL7', 
    #     'pageFirstCall': 'Y'} 
    # content_type = 'application/x-www-form-urlencoded; charset=UTF-8' 
    # response = requests.post(url=url, data=data, headers={'Content-Type': content_type}) 
    # resultJson = response.json() 
    # print(response.json()) 

    # holidays = [x['calnd_dd_dy'] for x in resultJson['block1']] 
    # return holidays