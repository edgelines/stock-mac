# Websoket Test
# def default_converter(o):
#     if isinstance(o, datetime):
#         return o.__str__()
    
# @app.websocket("/ws/StockData/{code}")
# async def websocket_stock_data(websocket: WebSocket, code: str):
#     await websocket.accept()
#     try:
#         while True :
#             db = client['Stock']
#             data = db[code]
#             mongo_data = list(data.find({}, {'_id': False}))
#             # await websocket.send_json(mongo_data)
#             json_data = json.dumps(mongo_data, default=default_converter)
#             await websocket.send_text(json_data)
#     except Exception as e:
#         # await websocket.send_text(f"Error: {str(e)}")
#         await websocket.send_json({"error ": str(e)})
#     finally:
#         await websocket.close()

# @app.websocket("/ws/StockSearch/Tracking")
# async def websocket_stock_search_tracking(websocket: WebSocket, date: date = None, skip: int = 0, limit: int = 3000):
#     await websocket.accept()
#     try:
#         while True :
#             db = client['Search']
#             data = db['Tracking']

#             query = {}
#             if date:
#                 startDay = datetime(date.year, date.month, date.day)
#                 endDay = datetime(date.year, date.month, date.day, 23, 59, 59)
#                 query['조건일'] = {"$gte": startDay, "$lte": endDay}

#             mongo_data = list(data.find(query, {'_id': False}).skip(skip).limit(limit))
#             json_data = json.dumps(mongo_data, default=default_converter)
#             await websocket.send_text(json_data)
#             # await websocket.send_json(mongo_data)
#     except Exception as e:
#         await websocket.send_text(f"Error: {str(e)}")
#     finally:
#         await websocket.close()