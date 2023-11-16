## Checkmate ver. Mac

import exchange_calendars as ecals 
from datetime import datetime, time as dt_time
from utils import holiday, send
import StockProcessing
import StockUpdate
import time

def is_market_open(current_time, start_time, end_time):
    return start_time <= current_time < end_time

def market_start(current_time, start_time, end_time):
    current_second = datetime.now().second
    if start_time < current_time <= end_time :
        send.errors( 'Mac-StockProcessing', 'Run' )
    time.sleep(60 - current_second)

def execute_tasks():
    current_minute = datetime.now().minute
    current_second = datetime.now().second
    
    if current_minute % 2 == 0:
        for func, func_name in functions_to_execute:
            execute_function(func, func_name, str(datetime.now()))
        print(f' ★  Done : {datetime.now()} // Runtime : {time.time() - start}')
        print('\n')
        check = 60 - current_second
        if check > 0:
            time.sleep(check)
    else:
        time.sleep(60 - current_second)

# 함수 실행 및 에러 처리
def execute_function(func, func_name, current_time):
    try:
        func()
        print(f"{func_name} : {str(time.time() - start)}")
    except Exception as e:
        print(f" =>  {func_name} : 에러 {current_time}")
        send.errors( f'{func_name} : {current_time}', e )

# 함수 리스트
functions_to_execute = [
    (StockUpdate.run, 'StockUpdate()'),
    (StockProcessing.run, 'StockProcessing()'),
]

if __name__ == '__main__':
    while True:
        XKRX = ecals.get_calendar("XKRX")
        holidays = holiday.run()
        today = datetime.now().date()
        start = time.time()  # 시작 시간 저장
        current_time = datetime.now().time()
        
        if XKRX.is_session(today.strftime("%Y-%m-%d")) == False or today in holidays:
            time.sleep(120)
            continue
        
        elif today.strftime("%Y%m%d") == holiday.get_ksat_date(today.year).strftime("%Y%m%d") or today.strftime("%Y%m%d") == holiday.get_first_weekday_of_year(today.year).strftime("%Y%m%d") :
            if is_market_open(current_time, dt_time(10, 2), dt_time(16, 48)):
                execute_tasks()
                continue
            else :
                time.sleep(120)
        else:
            if is_market_open(current_time, dt_time(9, 2), dt_time(15, 48)):
                execute_tasks()
                continue
            else :
                time.sleep(120)