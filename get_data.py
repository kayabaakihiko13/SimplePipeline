import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import constant as warna
from config import check_status,VALID_PERIODS

def get_data_currency(from_symbol:str,to_symbol:str,period:str="1Y"):
    """
    this code for scraping analytic

    Args:
        from_symbol (str): currency for 
        to_symbol (str): _description_
        period (str, optional): _description_. Defaults to "5Y".
    """
    if period not in VALID_PERIODS:
        raise ValueError(f"This bisa silakan check di sini{VALID_PERIODS}")
    url = "https://real-time-finance-data.p.rapidapi.com/currency-time-series"

    querystring = {"from_symbol":from_symbol,"to_symbol":to_symbol,
                   "period":period,"language":"en"}

    headers = {
        "x-rapidapi-key": "03d7f2d9ccmsha129b9e62cdf9fap153f12jsn0c736f818bc6",
        "x-rapidapi-host": "real-time-finance-data.p.rapidapi.com"
    }
    response = requests.get(url=url,headers=headers,params=querystring)
    status_code,_ = check_status(response)
    if response.status_code ==200:
        data = response.json()
        time_series_list = []
        # print(f"date {data['data']['time_series']}")
        
        for timestamp, ts_data in data['data']['time_series'].items():
            time_series_list.append({
                'timestamp': pd.to_datetime(timestamp).date(),
                'exchange_rate': ts_data['exchange_rate'],
                'change': ts_data['change']
            })
        df_result = pd.DataFrame(time_series_list)
        return df_result
    else:
        print(f"{warna.red}Error: {status_code} - {response.reason}{warna.reset_warna}")
    # check response
    


if __name__ =="__main__":
    result=get_data_currency("usd","idr","1Y")
    print(result.info())
    print(result.head(5))