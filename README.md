### Script
``` 
  ~/anaconda3/bin/python mac.py
  uvicorn main:app --reload --port=7901 --host=0.0.0.0
  gunicorn -k uvicorn.workers.UvicornWorker --access-logfile ./gunicorn-access.log main:app --bind 0.0.0.0:7901 --workers 8
  gunicorn -k uvicorn.workers.UvicornWorker --access-logfile ./gunicorn-access.log main:app --bind 0.0.0.0:7901 --workers 16
  chmod +x /Users/checkmate/Work/checkmateStock/utils/mongodb_backup.sh
```

### 2023.12.05 ~ 2023.12.07
- 기능제거, Gunicorn

### 2023.11.22
- 조건변경

### 2023.11.17
- 현재가 + DMI 연산 > multiprocessing 처리
- 현재가가 MA 위아래 연산 따로 처리. 
- Websocket Test

### 2023.11.16
- StockUpdate.py
  - DMI 계산추가 ( 전체 2분 30초 걸림 ) >> 멀티프로세싱 사용 >> 20초로 단축
- DB 자동백업 스크립트 작성

### 2023.11.15
- 부채비율, 유보율 가져오기 > Windows -> Mac DB Insert
- Mac.py > 작동시 카톡으로 동작여부 조건문 제거
- main.py
  - Tracking Api parameter 수정
  - ~~StockData pagination ( limit : 4000 )~~ > Stock 날짜 처리를 다시해야함.
- StockProcessing.py
  - 부채,유보율 merge 후 nan값 0 처리s
  - 과거 데이터 현재값 업데이트
  - DB .limit 처리 후 날짜 재정렬 처리
- StockUpdate.py
  - DMI 계산추가 ( 전체 2분 30초 걸림 )
- utils.py
  - ~~계산된 DMI 제외처리~~ > 원위치. ( DMI 컬럼 찾지못함 )

### 2023.11.12
- Commit.1
  - 부채비율, 유보율 Post API
- Commit.2 
  - 부채비율, 유보율 추가
- Commit.3
  - main.py => Query date

### 2023.11.10
- Commit.1
  - Mac.py 
    - 작동시 카톡으로 동작여부 조건문 오류 수정
    - 작동시간 조정
- Commit.2
  - StockProcessing.py
    - DMI 6,7 임시제건

### 2023.11.09
- Commit.1
  - Mac.py 작동시 카톡으로 동작여부 체크
- Commit.2
  - Mac.py 작동시간 오타
  - 보조지표 조건에 해당하는 종목들 DB insert/update
  - FastAPI 등록 
    - ?skip=0&limit=1000
- Commit.3
  - FastAPI - Table Name Fixed

### 2023.11.08
- Commit.1
  - DMI 6, 7 추가, 데이터셋 40개에서 10개로 줄임

### 2023.11.07
- Commit.1
  - holiday.py > API 변경

### 2023.11.06
- Commit.1
  - utils/send.py
    - Error Send KAKAO / URL
    - StockUpdate Result Post / URL
- Commit.2
  - mac.py : 작동코드 리팩토링
  - main.py : FastAPI
  - StockProcessing.py : df Null값 처리
  - holiday.py : 매년 1월 첫날, 수능날 찾기 추가
  - send.py : 서버전송 코드 수정
  - utils.py : indicator 에외값처리
- Commit.3
  - mac.py
    - Multiprocess 처리
    - 작동코드 리팩토링
  - StockProcessing.py > Post 요청 주소 변경
  - StockUpdate.py
    - IndexMA 추가, Post요청 처리
  - send.py
    - Post 요청 주소 변경

### 2023.11.04
- .env
- StockUpdate.py
  - 개별종목 업데이트
- StockProcessing.py
  - 멀티프로세싱사용하여 윌리엄스, DMI 처리 : 50초
- mac
  - execute