@app.get('/stock/list')
async def kakao_stock():
    db = client['Kakao']
    result = db['Msg']
    return list(result.find({}, {'_id':False}))

@app.get('/stock/daytrading')
async def kakao_stock():
    db = client['Kakao']
    result = db['DayTrading']
    return list(result.find({}, {'_id':False}))

class KakaoMsg(BaseModel):
    item : str
    price : str
    num : str

# #매매알림 전송
async def send_message_매매방(msg):
    # 100만원 이상일 경우의 메시지 전송 코드
    chatroom_name='주식거래방'
    bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
    bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
async def send_message_other_cases(msg):
    # 그 외의 경우 메시지 전송 코드
    room1 = ['윤아', '최지★']
    for name in room1 :
        bot.open_chatroom(chatroom_name=name)  # 채팅방 열기
        bot.kakao_sendtext(chatroom_name=name, text=msg, action='close')    # 메시지 전송

@app.post('/KakaoStockMsg', response_class=JSONResponse)
async def KakaoStockMsg(request:Request) :
    오늘 = datetime.today().strftime("%Y%m%d")
    req = await request.json() # post로 받은 데이터
    
    db = client['Kakao']
    result = db['Msg']
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
                    # msg = "🔴 삼 \n" +req['item'] + '\n' + str(format(req['price'], ','))+ ' 원'
                    await send_message_other_cases(msg) ## 그외
                    chatroom_name='주식거래방'
                    bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
                    bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송

                elif df['upDate'].values != 오늘 :
                    if 직전총액 + 새로산총액 > 300000 :
                        msg = (f"🔴 삼 \n★{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                        # msg = "🔴 삼 \n★ " +req['item'] + '\n' + str(format(req['price'], ','))+ ' 원'
                        chatroom_name='주식거래방'
                        bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
                        bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송

                    else :
                        msg = (f"🔴 보초 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                        # msg = "🔴 보초 \n" +req['item'] + '\n' + str(format(req['price'], ','))+ ' 원'
                        chatroom_name='주식거래방'
                        bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
                        bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송

                elif 새로산총액 > 300000 :
                    msg = (f"🔴 삼 \n★{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                    # msg = "🔴 삼 \n★ " +req['item'] + '\n' + str(format(req['price'], ','))+ ' 원'
                    chatroom_name='주식거래방'
                    bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
                    bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
                        
            else :
                보유수 = int(df['num']) - int(req['num'])
                수익률 = round((int(req['price']) - int(df['price'])) / int(df['price']) *100,2)
                # 일부매도
                if 보유수 > 0 :
                    result.update_one( {'item' : req['item'] },
                        { '$set': {
                            'num' : 보유수, 'sellDate' : 오늘 }
                        })
                    if df['sellDate'].values != 오늘 :
                        msg = (f"🔵 일부 정리 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                        # msg = "🔵 일부 정리 \n" + req['item'] + '\n수익률 : ' + str(수익률) + " %\n매도가 : " + str(format(req['price'], ',')) + ' 원'
                        if 직전총액 > 4000000 :
                            await asyncio.gather(send_message_other_cases(msg))
                            # await asyncio.gather(send_message_매매방(msg))
                            # for name in room1 :
                            #     bot.open_chatroom(name)  # 채팅방 열기
                            #     bot.kakao_sendtext(name, msg)    # 메시지 전송
                            chatroom_name='주식거래방'
                            bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
                            bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
                        else :
                            chatroom_name='주식거래방'
                            bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
                            bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
                
                # 전량매도
                else :
                    result.delete_one( {'item' : req['item'] } )
                    msg = (f"🔵 다 정리 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
                    # msg = "🔵 다 정리 \n" + req['item'] + '\n수익률 : ' + str(수익률) + " %\n매도가 : " + str(format(req['price'], ',')) + ' 원'
                    await send_message_매매방(msg)
                    if 직전총액 > 4000000 :
                        await asyncio.gather(send_message_매매방(msg))
        
        else :
            if req['class'] == '매수' :
                df = pd.DataFrame({
                    'item' : [req['item']],
                    'price' :[req['price']],
                    'num' : [req['num']],
                    'buyDate' : [오늘],
                    'upDate' : [오늘],
                    'sellDate' : ['']
                })
                
                result.insert_many(df.to_dict('records'))
                msg = "🔴 보초 \n" +req['item'] + '\n' + str(format(req['price'], ','))+ ' 원'
                chatroom_name='주식거래방'
                bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
                bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
            else : 
                pass
    else :
        if req['class'] == '매수' :
            df = pd.DataFrame({
                'item' : [req['item']],
                'price' :[req['price']],
                'num' : [req['num']],
                'buyDate' : [오늘],
                'upDate' : [오늘],
                'sellDate' : ['']
            })
            
            result.insert_many(df.to_dict('records'))
            msg = (f"🔴 보초 \n{req['item']}\n{str(format(req['num'], ','))} 주\n{str(format(req['price'], ','))} 원")
            # try :
            # except :
            #     msg = "🔴 보초 \n " +req['item'] +'\n'+ str(format(req['num'], ',')) + '주'+ '\n' + str(format(req['price'], ',')) + ' 원'
            chatroom_name='주식거래방'
            bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
            bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
        else : 
            pass
        
    # 당일 매매 현황 업데이트
    DayTrading = db['DayTrading']
    existing_entry = DayTrading.find_one({"item": req["item"], "trade_type": req["class"]})
    잔고 = result.find_one({"item": req["item"]})
    if existing_entry:
    # 업데이트할 내용을 정의합니다. 예를 들어, num을 더하고 price를 업데이트하려면 다음과 같이 작성할 수 있습니다.
        당일총액 = int(existing_entry['price']) * int(existing_entry['num'])
        평단 = ( 당일총액 + 새로산총액 ) / ( int(existing_entry['num'])+ int(req['num']) )
        보유수 = int(existing_entry['num'])+ int(req['num'])
        
        if req['class'] == '매수' :
            updated_data = { "$set": { "price": 평단, "num": 보유수 } }
        
        elif req['class'] == '매도' :
            수익률 = round((int(req['price']) - int(잔고['price'])) / int(잔고['price']) *100,2)
            updated_data = {
                "$set": {
                    "price": 평단,
                    "num": 보유수,
                    "profit" : 수익률
                    # 기타 업데이트할 필드
                }
            }
        # MongoDB 문서를 업데이트합니다.
        DayTrading.update_one({"_id": existing_entry["_id"]}, updated_data)
    else:
        # 새로운 데이터를 삽입합니다.
        데이터 = pd.DataFrame({'item': [req['item']],
                            'price': [req['price']],
                            'num': [req['num']],
                            'trade_type': [req['class']],
                            'profit': ['']})
        DayTrading.insert_many(데이터.to_dict('records'))    
        
    return {'inserted_id': str(result.inserted_id)}  # 결과 반환
    
@app.delete("/stock/delete/{item_name}")
async def stock_delete_item(item_name: str):
    db = client['Kakao']
    collection = db['Msg']
    result = collection.delete_one({"item": item_name})
    if result.deleted_count == 1:
        return {"message": "Item deleted successfully"}
    else:
        return {"message": "Item not found"}


@app.post('/hijonamContact', response_class=JSONResponse)
async def hijonamContact(request:Request) :
    req = await request.json() # post로 받은 데이터
    # msg = req
    # print(req)
    msg = "Contact Message"
    msg += '\n이름 : ' + req['name']
    msg += '\nemail : ' + req['email']
    msg += '\n' + req['subject']
    msg += '\n' + req['message']
    chatroom_name='hijonam'
    bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
    bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
    # bot.open_chatroom('hijonam')  # 채팅방 열기
    # bot.kakao_sendtext('hijonam', msg)    # 메시지 전송

# Mac >> Windows
@app.post('/sendKakaoMsg', response_class=JSONResponse)
async def sendKakaoMsg(request:Request) :
    req = await request.json() # post로 받은 데이터
    msg = "Mac"
    msg += '\n작업 : ' + req['msg']
    msg += '\n에러 : ' + req['error']
    chatroom_name='암호'
    bot.open_chatroom(chatroom_name=chatroom_name)  # 채팅방 열기
    bot.kakao_sendtext(chatroom_name=chatroom_name, text=msg)    # 메시지 전송
