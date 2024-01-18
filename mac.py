## Checkmate ver. Mac

import exchange_calendars as ecals 
from datetime import datetime, time as dt_time, timedelta
from utils import holiday, send
# import StockProcessing
# import StockUpdate
import time, re
from collections import Counter

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

def check_logs():
    log_file_path = './Api/temp/gunicorn-access.log'

    ip_pattern = re.compile(r'\d+\.\d+\.\d+\.\d+')
    ip_counter = Counter()

    with open(log_file_path, 'r') as file :
        for line in file:
            ip_address = ip_pattern.search(line)
            if ip_address:
                ip_counter[ip_address.group()] += 1

    result = []

    for ip, count in ip_counter.items():
        result.append({ 'IP' : ip, 'Count' : count })
    sorted_result = sorted(result, key=lambda x: x['Count'], reverse=True)
    
    send.check_logs( sorted_result )
    with open(log_file_path, 'w') as file:
        pass

    
# 함수 실행 및 에러 처리
def execute_function(func, func_name, current_time):
    try:
        func()
        print(f"{func_name} : {str(time.time() - start)}")
        end_time = datetime.now()
        elapsed_time = end_time - current_time
        sleep_time = timedelta(minutes=60) - elapsed_time

        if sleep_time.total_seconds() > 0 :
            time.sleep(sleep_time.total_seconds())

    except Exception as e:
        print(f" =>  {func_name} 에러 : {current_time}")
        send.errors( f'{func_name}', e )

# 함수 리스트
functions_to_execute = [
    (check_logs, 'check_logs()'),
    # (StockUpdate.run, 'StockUpdate()'),
    # (StockProcessing.run, 'StockProcessing()'),
]

if __name__ == '__main__':
    while True:
        current_time = datetime.now().time()
        start = time.time()  # 시작 시간 저장
        
        if dt_time(23, 00) < current_time <= dt_time(23,59) :
            print('run')
            execute_tasks()
            continue

        else:
            time.sleep(60 * 60)


        # XKRX = ecals.get_calendar("XKRX")
        # holidays = holiday.run()
        # today = datetime.now().date()
        # current_time = datetime.now().time()
        
        # if XKRX.is_session(today.strftime("%Y-%m-%d")) == False or today in holidays:
        #     time.sleep(120)
        #     continue
        
        # elif today.strftime("%Y%m%d") == holiday.get_ksat_date(today.year).strftime("%Y%m%d") or today.strftime("%Y%m%d") == holiday.get_first_weekday_of_year(today.year).strftime("%Y%m%d") :
        #     if is_market_open(current_time, dt_time(10, 2), dt_time(16, 48)):
        #         execute_tasks()
        #         continue
        #     elif is_market_open(current_time, dt_time(17, 10), dt_time(17, 15)):
        #         execute_tasks()
        #         continue
        #     else :
        #         time.sleep(120)
        # else:
        #     if is_market_open(current_time, dt_time(9, 2), dt_time(15, 48)):
        #         execute_tasks()
        #         continue
        #     elif is_market_open(current_time, dt_time(16, 10), dt_time(16, 15)):
        #         execute_tasks()
        #         continue
        #     else :
        #         time.sleep(120)