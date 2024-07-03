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
        logging.info(f'calling dataframe result {indicator_symbol} dataframe {dataframe} ')
        if dataframe is not None:
            dataframes.append(dataframe)
        time.sleep(1.5)
    if len(dataframes) > 0:
        combined_df = pd.concat(dataframes)
        return combined_df
    
    return pd.DataFrame()
