from unittest import TestCase
from unittest.mock import patch, MagicMock, call
from azure.functions import HttpRequest, HttpResponse
from io import BytesIO
import functools
import pandas as pd
from tests.test_utils.indicators import get_mock_indicator, get_mock_indicator_historical_dataframe
from utils.exceptions import IndicatorException

INIT_DATE = '1960-01-01'
END_DATE = '2000-01-01'

def mock_decorator(*args, **kwargs):
    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            req = args[0]
            req.__setattr__('header_params', {
                "init_date": INIT_DATE,
                "end_date": END_DATE,
                "indicator_date": INIT_DATE
            })
            return f(*args, **kwargs)
        return decorated_function
    return decorator

class TestExtractIndicators(TestCase):

    @classmethod
    def setUpClass(cls):
        patch("validations.validators.http_validator.validate", mock_decorator).start()
        patch("tradingeconomics.login", MagicMock()).start()
        patch("azure.storage.filedatalake.DataLakeServiceClient", MagicMock()).start()
        patch("azure.keyvault.secrets.SecretClient", MagicMock()).start()
        global get_daily_indicators, get_historical_indicators, extract_indicator_daily, extract_indicator_historical, extract_indicators_historical, extract_indicators_daily

        from http_triggers.extract_indicators import get_daily_indicators, extract_indicator_daily, extract_indicator_historical, extract_indicators_historical, extract_indicators_daily, get_historical_indicators

    @classmethod
    def tearDownClass(cls):
        patch.stopall()

    @patch('http_triggers.extract_indicators.get_historical_indicators')
    @patch('http_triggers.extract_indicators.get_trading_economics_indicators_to_request')
    def test_extract_indicators_historical(self, mock_get_trading_economics_indicators_to_request, mock_get_historical_indicators):
        mock_get_trading_economics_indicators_to_request.return_value = ['COM:CO1']
        req = HttpRequest(
            method='GET',
            url='localhost/indicator/historical/all',
            body={}
        )

        extract_indicators_historical.build().get_user_function()(req)

        mock_get_trading_economics_indicators_to_request.assert_called_once()
        mock_get_historical_indicators.assert_called_once_with(
             ['COM:CO1'],
             INIT_DATE,
             END_DATE
        )

    @patch('http_triggers.extract_indicators.get_historical_indicators')
    @patch('http_triggers.extract_indicators.get_trading_economics_indicators_to_request')
    def test_extract_indicator_historical(self, mock_get_trading_economics_indicators_to_request, mock_get_historical_indicators):
        mock_get_trading_economics_indicators_to_request.return_value = ['COM:CO1']
        req = HttpRequest(
            method='GET',
            url='localhost/indicator/historical/test_symbol',
            route_params={'symbol': 'test_symbol'},
            body={}
        )
        req.__setattr__('path_params', {"symbol": 'test_symbol'})

        extract_indicator_historical.build().get_user_function()(req)

        mock_get_historical_indicators.assert_called_once_with(
             ['test_symbol'],
             INIT_DATE,
             END_DATE
        )

        
    @patch('http_triggers.extract_indicators.get_daily_indicators')
    def test_extract_indicator_daily(self, mock_get_daily_indicators):
        req = HttpRequest(
            method='GET',
            url='localhost/indicator/daily/test_symbol',
            route_params={'symbol': 'test_symbol'},
            body={}
        )
        req.__setattr__('path_params', {"symbol": 'test_symbol'})

        extract_indicator_daily.build().get_user_function()(req)

        mock_get_daily_indicators.assert_called_once_with(
             ['test_symbol'],
             INIT_DATE,
        )

    @patch('http_triggers.extract_indicators.get_daily_indicators')
    @patch('http_triggers.extract_indicators.get_trading_economics_indicators_to_request')
    def test_extract_indicators_daily(self, mock_get_trading_economics_indicators_to_request, mock_get_daily_indicators):
        mock_get_trading_economics_indicators_to_request.return_value = ['COM:CO1']
        req = HttpRequest(
            method='GET',
            url='localhost/indicator/daily/all',
            body={}
        )
        extract_indicators_daily.build().get_user_function()(req)

        mock_get_daily_indicators.assert_called_once_with(
             ['COM:CO1'],
             INIT_DATE,
        )

    @patch("http_triggers.extract_indicators.save_dataframe")
    @patch("http_triggers.extract_indicators.get_filename")
    @patch("http_triggers.extract_indicators.get_indicator_info")
    @patch("http_triggers.extract_indicators.get_indicator_historical")
    def test_get_historical_indicators_for_zero_indicator(self, mock_get_indicator_historical, mock_get_indicator_info, mock_get_filename, mock_save_dataframe):
        mock_indicators = []

        get_historical_indicators(mock_indicators, INIT_DATE, END_DATE)

        mock_save_dataframe.assert_not_called()
        mock_get_indicator_historical.assert_not_called()
        mock_get_indicator_info.assert_not_called()
        mock_save_dataframe.assert_not_called()
        mock_get_filename.assert_not_called()

    @patch("http_triggers.extract_indicators.save_dataframe")
    @patch("http_triggers.extract_indicators.get_filename")
    @patch("http_triggers.extract_indicators.get_indicator_info")
    @patch("http_triggers.extract_indicators.get_indicator_historical")
    def test_get_historical_indicators_successful_for_one_indicator(self, mock_get_indicator_historical, mock_get_indicator_info, mock_get_filename, mock_save_dataframe):
        mock_indicators = ['COM:CO1']
        mock_historical = pd.DataFrame()
        mock_get_filename.return_value = 'TRADING_ECONOMICS_COM:CO1.parquet'
        mock_get_indicator_historical.return_value = mock_historical
        mock_get_indicator_info.return_value = {"Symbol": "COM:CO1", "Type": "commodity"}

        get_historical_indicators(mock_indicators, INIT_DATE, END_DATE)

        mock_save_dataframe.assert_called_with(
            mock_historical,
            'TRADING_ECONOMICS_COM:CO1.parquet',
            'commodity'
        )

    @patch("http_triggers.extract_indicators.save_dataframe")
    @patch("http_triggers.extract_indicators.get_filename")
    @patch("http_triggers.extract_indicators.get_indicator_info")
    @patch("http_triggers.extract_indicators.get_indicator_historical")
    def test_get_historical_indicators_successful_for_more_than_one_indicator(self, mock_get_indicator_historical, mock_get_indicator_info, mock_get_filename, mock_save_dataframe):
        mock_indicators = ['COM:CO1', 'BON:BO1']
        mock_historical = pd.DataFrame()
        mock_get_filename.side_effect = [
            'TRADING_ECONOMICS_COM:CO1.parquet',
            'TRADING_ECONOMICS_BON:BO1.parquet'
        ]
        mock_get_indicator_historical.return_value = mock_historical
        mock_get_indicator_info.side_effect  = [
            {"Symbol": "COM:CO1", "Type": "commodity"},
            {"Symbol": "BON:BO1", "Type": "bond"}
        ]
        get_historical_indicators(mock_indicators, INIT_DATE, END_DATE)

        mock_save_dataframe.assert_has_calls([
            call(mock_historical, 'TRADING_ECONOMICS_COM:CO1.parquet', 'commodity'),
            call(mock_historical, 'TRADING_ECONOMICS_BON:BO1.parquet', 'bond')
        ])

    @patch("http_triggers.extract_indicators.save_dataframe")
    @patch("http_triggers.extract_indicators.get_filename")
    @patch("http_triggers.extract_indicators.get_indicator_info")
    @patch("http_triggers.extract_indicators.get_indicator_historical")
    def test_get_historical_indicators_successful_for_more_than_one_indicator(self, mock_get_indicator_historical, mock_get_indicator_info, mock_get_filename, mock_save_dataframe):
        mock_indicators = ['COM:CO1', 'BON:BO1']
        mock_historical = pd.DataFrame()
        mock_get_filename.side_effect = [
            'TRADING_ECONOMICS_COM:CO1.parquet',
            'TRADING_ECONOMICS_BON:BO1.parquet'
        ]
        mock_get_indicator_historical.return_value = mock_historical
        mock_get_indicator_info.side_effect  = [
            {"Symbol": "COM:CO1", "Type": "commodity"},
            {"Symbol": "BON:BO1", "Type": "bond"}
        ]
        get_historical_indicators(mock_indicators, INIT_DATE, END_DATE)

        mock_save_dataframe.assert_has_calls([
            call(mock_historical, 'TRADING_ECONOMICS_COM:CO1.parquet', 'commodity'),
            call(mock_historical, 'TRADING_ECONOMICS_BON:BO1.parquet', 'bond')
        ])

    @patch("http_triggers.extract_indicators.get_indicator_info")
    def test_get_historical_indicators_not_found(self, mock_get_indicator_info):
        mock_get_indicator_info.return_value = None

        self.assertRaises(IndicatorException, get_historical_indicators, ['COM:CO1'], INIT_DATE, END_DATE)

    @patch("http_triggers.extract_indicators.save_dataframe")
    @patch("http_triggers.extract_indicators.get_filename")
    @patch("http_triggers.extract_indicators.get_indicator_info")
    @patch("http_triggers.extract_indicators.get_indicator_historical")
    def test_get_daily_indicators_for_zero_indicator(self, mock_get_indicator_historical, mock_get_indicator_info, mock_get_filename, mock_save_dataframe):
        mock_indicators = []

        get_daily_indicators(mock_indicators, END_DATE)

        mock_save_dataframe.assert_not_called()
        mock_get_indicator_historical.assert_not_called()
        mock_get_indicator_info.assert_not_called()
        mock_save_dataframe.assert_not_called()
        mock_get_filename.assert_not_called()

    @patch("http_triggers.extract_indicators.sort_dataframe_by_date")
    @patch("http_triggers.extract_indicators.update_dataframe")
    @patch("http_triggers.extract_indicators.get_dataframe")
    @patch("http_triggers.extract_indicators.save_dataframe")
    @patch("http_triggers.extract_indicators.get_filename")
    @patch("http_triggers.extract_indicators.get_indicator_info")
    @patch("http_triggers.extract_indicators.get_indicator_historical")
    def test_get_daily_indicators_successful_for_one_indicator(self, mock_get_indicator_historical, mock_get_indicator_info, mock_get_filename, mock_save_dataframe, mock_get_dataframe, mock_update_dataframe, mock_sort_dataframe_by_date):
        mock_indicators = ['COM:CO1']
        mock_dataframe = pd.DataFrame()
        mock_get_dataframe.return_value = mock_dataframe
        mock_update_dataframe.return_value = mock_dataframe
        mock_sort_dataframe_by_date.return_value = mock_dataframe
        mock_get_filename.return_value = 'TRADING_ECONOMICS_COM:CO1.parquet'
        mock_get_indicator_historical.return_value = mock_dataframe
        mock_get_indicator_info.return_value = {"Symbol": "COM:CO1", "Type": "commodity"}

        get_daily_indicators(mock_indicators, INIT_DATE)

        mock_save_dataframe.assert_called_with(
            mock_dataframe,
            'TRADING_ECONOMICS_COM:CO1.parquet',
            'commodity'
        )


    @patch("http_triggers.extract_indicators.sort_dataframe_by_date")
    @patch("http_triggers.extract_indicators.update_dataframe")
    @patch("http_triggers.extract_indicators.get_dataframe")
    @patch("http_triggers.extract_indicators.save_dataframe")
    @patch("http_triggers.extract_indicators.get_filename")
    @patch("http_triggers.extract_indicators.get_indicator_info")
    @patch("http_triggers.extract_indicators.get_indicator_historical")
    def test_get_daily_indicators_successful_for_more_than_one_indicator(self, mock_get_indicator_historical, mock_get_indicator_info, mock_get_filename, mock_save_dataframe, mock_get_dataframe, mock_update_dataframe, mock_sort_dataframe_by_date):
        mock_indicators = ['COM:CO1', "BON:BO1"]
        mock_dataframe = pd.DataFrame()
        mock_sort_dataframe_by_date.return_value = mock_dataframe
        mock_get_dataframe.return_value = mock_dataframe
        mock_update_dataframe.return_value = mock_dataframe
        mock_get_filename.side_effect = [
            'TRADING_ECONOMICS_COM:CO1.parquet',
            'TRADING_ECONOMICS_BON:BO1.parquet'
        ]
        mock_get_indicator_historical.return_value = mock_dataframe
        mock_get_indicator_info.side_effect  = [
            {"Symbol": "COM:CO1", "Type": "commodity"},
            {"Symbol": "BON:BO1", "Type": "bond"}
        ]

        get_daily_indicators(mock_indicators, INIT_DATE)

        mock_save_dataframe.assert_has_calls([
            call(mock_dataframe, 'TRADING_ECONOMICS_COM:CO1.parquet', 'commodity'),
            call(mock_dataframe, 'TRADING_ECONOMICS_BON:BO1.parquet', 'bond')
        ])

    @patch("http_triggers.extract_indicators.get_indicator_info")
    def test_get_daily_indicators_not_found(self, mock_get_indicator_info):
        mock_get_indicator_info.return_value = None

        self.assertRaises(IndicatorException, get_daily_indicators, ['COM:CO1'], END_DATE)
