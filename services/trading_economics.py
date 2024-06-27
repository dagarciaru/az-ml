import time
import tradingeconomics as te
import pandas as pd
from utils.dates import get_year_windows
from utils.system import get_trading_economics_api_key
import logging

te.login(get_trading_economics_api_key())


def get_indicator_historical(indicator_symbol, init_date, end_date):
    year_windows = get_year_windows(init_date, end_date, 30)

    dataframes = []
    for year_window in year_windows:
        logging.info(f'calling TE for {indicator_symbol} between {year_window["initDate"]} and {year_window["endDate"]}')
        dataframe = te.getHistorical(
            symbol=indicator_symbol,
            initDate=year_window['initDate'],
            endDate=year_window['endDate'],
            output_type='df'
        )
        if dataframe is not None:
            dataframes.append(dataframe)
        time.sleep(1.5)
    if len(dataframes) > 0:
        combined_df = pd.concat(dataframes)
        combined_df['DateTime'] = pd.to_datetime(combined_df['DateTime'])  
        combined_df = combined_df.sort_values(by='DateTime')  
        combined_df['DateTime'] = combined_df['DateTime'].dt.strftime('%Y-%m-%dT%H:%M:%S')  
        return combined_df
    
    return pd.DataFrame()

def get_indicators_info(indicators_symbols):
    response = te.getMarketsBySymbol(symbols = indicators_symbols)
    time.sleep(1.5)
    return response

    logging.info(f'calling TE for {serie_id} between {init_date} and {end_date}')
    
    series = fred.get_series(serie_id, observation_start=init_date, observation_end=end_date)
    
    df = series.reset_index()
    df.columns = ['Date', 'Value']
    
    df['Symbol'] = serie_id
    df['Date'] = df['Date'].astype(str)
    df['Value'] = df['Value'].astype('float64')
    
    df = df[['Symbol', 'Date', 'Value']]

    return df