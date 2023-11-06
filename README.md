### Todo-List
- execute 
- FastAPI Setting

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