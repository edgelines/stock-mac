import requests
import os
from dotenv import load_dotenv
load_dotenv()

headers = {
    "Content-Type": "application/json"
}
# def errors(msg, e):
#     api_url = os.getenv("API_URL_KAKAO")
#     data_list = { 'msg' : msg ,'error' : e}
#     response = requests.post(api_url, json=data_list, headers=headers)
#     if response.status_code == 200:
#         print(f"Error Msg Send..")

def check_logs(msg):
    api_url = os.getenv("API_URL_KAKAO")
    data_list = { 'msg' : msg }
    response = requests.post(f'{api_url}/', json=data_list, headers=headers)
    if response.status_code == 200:
        print(f"Error Msg Send..")

def trading_msg(msg):
    api_url = os.getenv("API_URL_KAKAO")
    data_list = { 'msg' : msg }
    response = requests.post(f'{api_url}/trading', json=data_list, headers=headers)
    if response.status_code == 200:
        print(f"Msg Send.. {msg}")

# def data(data_list, db):
#     api_url = os.getenv("API_URL_POST")
#     response = requests.post(f'{api_url}/{db}', json=data_list, headers=headers)
        
#     if response.status_code == 200:
#         pass
#         # print(f"Data successfully sent.")
#     else:
#         errors('Mac > Windows 실패 : ', response.status_code)