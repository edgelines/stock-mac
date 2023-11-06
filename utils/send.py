import requests
import os
from dotenv import load_dotenv
load_dotenv()

headers = {
    "Content-Type": "application/json"
}
def errors(msg, e):
    api_url = os.getenv("API_URL_KAKAO")
    data_list = { 'msg' : msg ,'error' : e}
    response = requests.post(api_url, json=data_list, headers=headers)
    if response.status_code == 200:
        print(f"Data successfully sent.")
    
def data(data_list):
    api_url = os.getenv("API_URL_STOCK_UPDATE")
    response = requests.post(api_url, json=data_list, headers=headers)
        
    if response.status_code == 200:
        print(f"Data successfully sent.")
    else:
        errors('Mac > Windows 실패 : ', response.status_code)