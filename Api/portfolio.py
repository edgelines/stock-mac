from fastapi import APIRouter, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
import pymongo
import pandas as pd
import logging
from utils import send
logging.basicConfig(level=logging.INFO)

router = APIRouter()
client = pymongo.MongoClient(host=['localhost:27017'])
# client = pymongo.MongoClient(host=['192.168.0.3:27017'])

@router.post('/stock', response_class=JSONResponse)
async def KakaoStockMsg(request:Request) :
    오늘 = datetime.today().strftime("%Y%m%d")
    req = await request.json() # post로 받은 데이터
    
    result = client.Treding.StockPortfolio

    보유종목 = pd.DataFrame(result.find({}, {'_id':False}))
    새로산총액 = int(req['price']) * int(req['num'])
    
    if len(보유종목) > 0:
        종목리스트 = 보유종목.item.to_list()
        
        if req['item'] in 종목리스트 :
            df = 보유종목[보유종목['item'] == req['item']]
            직전총액 = int(df['price']) * int(df['num'])
                    
            if req['class'] == '매수' :
                평단 = (직전총액 + 새로산총액 ) / (int(df['num'])+ int(req['num']))
                보유수 = int(df['num'])+ int(req['num'])
                result.update_one( {'item' : req['item'] },
                    { '$set': {
                        'price' : 평단, 'num' : 보유수, 'upDate' : 오늘 }
                    })

                if 직전총액 + 새로산총액 > 4000000 and df['upDate'].values != 오늘 :
                    msg = (f"🔴 삼 \n★{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                    send.trading_msg(msg)        
                    

                elif df['upDate'].values != 오늘 :
                    if 직전총액 + 새로산총액 > 300000 :
                        msg = (f"🔴 삼 \n★{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                        send.trading_msg(msg)

                    else :
                        msg = (f"🔴 보초 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                        send.trading_msg(msg)

            else :
                try :
                    보유수 = int(df['num']) - int(req['num'])
                    if 보유수 > 0 :
                        result.update_one( {'item' : req['item'] },
                            { '$set': {
                                'num' : 보유수, 'sellDate' : 오늘 }
                            })
                        if df['sellDate'].values != 오늘 :
                            msg = (f"🔵 일부 정리 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                            send.trading_msg(msg)
                except : pass
                
                # 전량매도
                else :
                    result.delete_one( {'item' : req['item'] } )
                    msg = (f"🔵 다 정리 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                    send.trading_msg(msg)
        
        else :
            if req['class'] == '매수' :
                data = {
                    'item' : req['item'],
                    'price' :req['price'],
                    'num' : req['num'],
                    'buyDate' : 오늘,
                    'upDate' : 오늘,
                    'sellDate' : ['']
                }
                
                result.insert_one(data)
                msg = "🔴 보초 \n" +req['item'] + '\n' + str(format(req['price'], ','))+ ' 원'
                send.trading_msg(msg)
            else : 
                pass
    else :
        if req['class'] == '매수' :
            data = {
                'item' : req['item'],
                'price' :req['price'],
                'num' : req['num'],
                'buyDate' : 오늘,
                'upDate' : 오늘,
                'sellDate' : ['']
            }
            
            result.insert_one(data)

            msg = f"🔴 보초 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원"
            send.trading_msg(msg)
            
        else : 
            pass
        
    # # 당일 매매 현황 업데이트
    # DayTrading = db['DayTrading']
    # existing_entry = DayTrading.find_one({"item": req["item"], "trade_type": req["class"]})
    # 잔고 = result.find_one({"item": req["item"]})
    # if existing_entry:
    # # 업데이트할 내용을 정의합니다. 예를 들어, num을 더하고 price를 업데이트하려면 다음과 같이 작성할 수 있습니다.
    #     당일총액 = int(existing_entry['price']) * int(existing_entry['num'])
    #     평단 = ( 당일총액 + 새로산총액 ) / ( int(existing_entry['num'])+ int(req['num']) )
    #     보유수 = int(existing_entry['num'])+ int(req['num'])
        
    #     if req['class'] == '매수' :
    #         updated_data = { "$set": { "price": 평단, "num": 보유수 } }
        
    #     elif req['class'] == '매도' :
    #         수익률 = round((int(req['price']) - int(잔고['price'])) / int(잔고['price']) *100,2)
    #         updated_data = {
    #             "$set": {
    #                 "price": 평단,
    #                 "num": 보유수,
    #                 "profit" : 수익률
    #                 # 기타 업데이트할 필드
    #             }
    #         }
    #     # MongoDB 문서를 업데이트합니다.
    #     DayTrading.update_one({"_id": existing_entry["_id"]}, updated_data)
    # else:
    #     # 새로운 데이터를 삽입합니다.
    #     데이터 = pd.DataFrame({'item': [req['item']],
    #                         'price': [req['price']],
    #                         'num': [req['num']],
    #                         'trade_type': [req['class']],
    #                         'profit': ['']})
    #     DayTrading.insert_many(데이터.to_dict('records'))    
        
    # return {'inserted_id': str(result.inserted_id)}  # 결과 반환