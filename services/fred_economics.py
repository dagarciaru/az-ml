import time
import tradingeconomics as te
import pandas as pd
from fredapi import Fred
from utils.system import  get_fred_api_key
import logging

fred_api_key = get_fred_api_key()

fred = Fred(api_key=fred_api_key)

def get_indicator_historical_fred_series(serie_id, init_date = None, end_date = None):
    logging.info(f'calling TE for {serie_id} between {init_date} and {end_date}')
    
    series = fred.get_series(serie_id, observation_start=init_date, observation_end=end_date)
    
    df = series.reset_index()
    df.columns = ['Date', 'Value']
    
    df['Symbol'] = serie_id
    df['Date'] = df['Date'].astype(str)
    df['Value'] = df['Value'].astype('float64')
    
    df = df[['Symbol', 'Date', 'Value']]

    return df