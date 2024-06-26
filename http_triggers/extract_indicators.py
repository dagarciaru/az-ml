import json
from os import environ
import azure.functions as func
from services.trading_economics import get_indicator_historical
from services.fred_economics import get_indicator_historical_fred_series

import logging
from validations.validators.http_validator import validate, ValidationType
from validations.models.get_indicators import GetHistoricalHeaders, GetPathParam, GetDailyHeaders
from utils.system import get_filename, get_trading_economics_indicators_to_request
from utils.dataframes import get_dataframe, save_dataframe, update_dataframe, sort_dataframe_by_date
from utils.exceptions import IndicatorException
from services.datalake import copy_file_to_target_datalake
from utils.dataframes import convert_to_in_memory_parquet

blueprint_extract_indicators = func.Blueprint() 

FILE_PREFIX = 'TRADING_ECONOMICS'
FILE_FRED_PREFIX = 'FRED'
file_economics_path = environ.get("DATALAKE_TARGET_BASE_ECONOMICS_FOLDER")
file_fred_path = environ.get("DATALAKE_TARGET_BASE_FRED_FOLDER")
fred_series_info = environ.get("FRED_SERIES_INFO")

def get_historical_indicators(indicators, init_date, end_date):
    for indicator_symbol, indicator_path in indicators.items():
        print(indicator_symbol, indicator_path)

        logging.info(f"Requesting indicator close price for {indicator_symbol} from {init_date} to {end_date}")
        dataframe = get_indicator_historical(indicator_symbol, init_date, end_date)

        file_name = get_filename(prefix = 'TRADING_ECONOMICS', name = indicator_symbol, ext= "parquet")

        logging.info(f"Saving changes into parquet file {file_name}")
        save_dataframe(dataframe, file_name, indicator_path)

        file_binary = convert_to_in_memory_parquet(dataframe)
       
        copy_file_to_target_datalake(file_binary, file_name, indicator_path,file_economics_path)


def get_daily_indicators(indicators, request_date):

    for indicator_symbol, indicator_path in indicators.items():

        file_name = get_filename(prefix=FILE_PREFIX, name=indicator_symbol, ext='parquet')

        logging.info(f'Retrieving parquet {file_name} from Datalake')
        datalake_dataframe = get_dataframe(file_name, indicator_path)

        logging.info(f"Requesting indicator close price for {indicator_symbol} at date {request_date}")
        results_dataframe = get_indicator_historical(indicator_symbol, request_date, request_date)

        logging.info(f"Got indicator {results_dataframe.to_string()}")

        logging.info(f"Updating {file_name} parquet with new data")
        updated_dataframe = update_dataframe(datalake_dataframe, results_dataframe, 'Date')

        sorted_dataframe = sort_dataframe_by_date(updated_dataframe, 'Date')
        logging.info(f"Got a new parquet with {len(updated_dataframe)} rows")

        logging.info(f"Saving changes into parquet file {file_name}")
        save_dataframe(sorted_dataframe, file_name, indicator_path)

        file_binary = convert_to_in_memory_parquet(sorted_dataframe)
        copy_file_to_target_datalake(file_binary, file_name, indicator_path,file_economics_path)

def get_historical_fred_indicators(series_info, init_date, end_date):
     for serie_id, serie_path in series_info.items():
         logging.info(f"Requesting indicator Fred  for {serie_id} from {init_date} to {end_date}")
         dataframe = get_indicator_historical_fred_series(serie_id, init_date, end_date)
         file_name = get_filename(prefix=FILE_FRED_PREFIX, name=serie_id, ext='parquet')
         logging.info(f"filename {file_name} dataframe {dataframe} from {init_date} to {end_date}")
         
         logging.info(f"Saving changes into parquet file {file_name}")
         #save_dataframe(dataframe, file_name, "indicator_path")
        
         file_binary = convert_to_in_memory_parquet(dataframe)
         copy_file_to_target_datalake(file_binary, file_name, serie_path,file_fred_path)

         

@blueprint_extract_indicators.route(route="indicator/historical/all", auth_level=func.AuthLevel.ANONYMOUS)
@validate(GetHistoricalHeaders, ValidationType.HEADERS)
def extract_indicators_historical(req: func.HttpRequest) -> func.HttpResponse:
    init_date = req.header_params.get('init_date')
    end_date = req.header_params.get('end_date')
    logging.info(f'Requesting indicator historical between dates {init_date} and {end_date} for all system indicators')
    indicators = get_trading_economics_indicators_to_request()
    get_historical_indicators(indicators, init_date, end_date)
    return func.HttpResponse('Successfull')


@blueprint_extract_indicators.route(route="indicator/historical/{symbol}", auth_level=func.AuthLevel.ANONYMOUS)
@validate(GetHistoricalHeaders, ValidationType.HEADERS)
@validate(GetPathParam, ValidationType.PATH_PARAMS)
def extract_indicator_historical(req: func.HttpRequest) -> func.HttpResponse:
    init_date = req.header_params.get('init_date')
    end_date = req.header_params.get('end_date')
    indicator_symbol = req.path_params['symbol']

    indicators = get_trading_economics_indicators_to_request()
    if indicator_symbol not in indicators:
        raise IndicatorException(f'{indicator_symbol} was not found', status_code=404)

    get_historical_indicators({indicator_symbol: indicators[indicator_symbol]}, init_date, end_date)

    return func.HttpResponse('Successfull')



@blueprint_extract_indicators.route(route="indicator/daily/all", auth_level=func.AuthLevel.ANONYMOUS)
@validate(GetDailyHeaders, ValidationType.HEADERS)
def extract_indicators_daily(req: func.HttpRequest) -> func.HttpResponse:
    request_date = req.header_params.get('indicator_date')
    
    indicators = get_trading_economics_indicators_to_request()

    get_daily_indicators(indicators, request_date)

    return func.HttpResponse('Successfull')

@blueprint_extract_indicators.route(route="indicator/daily/{symbol}", auth_level=func.AuthLevel.ANONYMOUS)
@validate(GetPathParam, ValidationType.PATH_PARAMS)
@validate(GetDailyHeaders, ValidationType.HEADERS)
def extract_indicator_daily(req: func.HttpRequest) -> func.HttpResponse:
    request_date = req.header_params.get('indicator_date')
    indicator_symbol = req.path_params.get('symbol')
    
    indicators = get_trading_economics_indicators_to_request()
    if indicator_symbol not in indicators:
        raise IndicatorException(f'{indicator_symbol} was not found', status_code=404)

    get_daily_indicators({indicator_symbol: indicators[indicator_symbol]}, request_date)

    return func.HttpResponse('Successfull')



@blueprint_extract_indicators.route(route="fred/historical", auth_level=func.AuthLevel.ANONYMOUS)
@validate(GetHistoricalHeaders, ValidationType.HEADERS)
def extract_fred_historical(req: func.HttpRequest) -> func.HttpResponse:
    init_date = req.header_params.get('init_date')
    end_date = req.header_params.get('end_date')
    logging.info(f'Requesting FRED historical data between dates {init_date} and {end_date}')
    logging.info(f'fred_series_info {fred_series_info} and {end_date}')
    series_info = json.loads(fred_series_info)

    get_historical_fred_indicators(series_info, init_date, end_date)
    return func.HttpResponse('Successfull')
