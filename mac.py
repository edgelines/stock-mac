## Checkmate ver. Mac

import exchange_calendars as ecals 
from datetime import datetime
from utils import holiday 
import StockProcessing
import StockUpdate

# 함수 실행 및 에러 처리
def execute_function(func, func_name, current_time):
    try:
        func()
        print(f"{func_name} : {str(time.time() - start)}")
    except Exception as e:
        print(f" =>  {func_name} : 에러 {current_time}")
        print('\n')
        error.Log((f'{func_name} : {current_time} '), e)

# 함수 리스트
functions_to_execute = [
    (StockProcessing.run, 'StockProcessing()'),
    (StockUpdate.run, 'StockUpdate()'),
]


while True:
    현재시각 = str(datetime.today().now())
    start = time.time()  # 시작 시간 저장
    current_hour = datetime.today().now().hour
    current_minute = datetime.today().now().minute
    current_second = datetime.today().now().second
    open_time = datetime.strptime("09:02", "%H:%M").time()
    close_time = datetime.strptime("15:59", "%H:%M").time()
    start_time = datetime.now()
    current_time = start_time.time()

    today = datetime.today().strftime("%Y-%m-%d")
    XKRX = ecals.get_calendar("XKRX") # 한국 코드 print(XKRX.is_session("2021-09-20")) # 2021-09-20 은 개장일인지 확인
    holidays = holiday.run()
    if XKRX.is_session(today) == False or today not in holidays :
        continue

    else :
        ## 장 오픈 시각 
        ## 매월 1월 첫날과 수능일엔 10시 시작 16:30 종료
        print('영업일')
         if open_time <= current_time < close_time :       
            if current_minute % 5 == 0 :
                for func, func_name in functions_to_execute:
                    execute_function(func, func_name, 현재시각)
                print(' ★  Done : ' + 현재시각 + ' // Runtime : ' + str(time.time() - start))
                print('\n')
                time.sleep(60 - current_second)
            
            else :
                time.sleep(60 - current_second)
    
            continue
        
        else:
            continue
        # if 매월첫날 and 수능일 == True : 
        #     print('오픈 10시, 종료 16시 30분')
    