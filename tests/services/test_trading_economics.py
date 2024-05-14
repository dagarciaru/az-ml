from unittest import TestCase
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal

patch('tradingeconomics.login').start()
patch('azure.keyvault.secrets.SecretClient').start()
from services.trading_economics import get_indicator_historical, get_indicator_info, get_indicators_info
from tests.test_utils.indicators import get_mock_indicator_historical_dataframe, get_mock_indicator

class TestTradingEconomics(TestCase):

    @patch('tradingeconomics.getHistorical')
    @patch('utils.dates.get_year_windows')
    def test_get_comodity_full_historical_one_te_api_call(self, mock_get_year_windows, mock_getHistorical):
        mock_historical_dataframe = get_mock_indicator_historical_dataframe(3)
        mock_get_year_windows.return_value = [
            {"init_date": '2024-03-07', "end_date": '2024-03-07'}
        ]
        mock_getHistorical.return_value = mock_historical_dataframe

        dataframes = get_indicator_historical(
            mock_historical_dataframe.loc[0, 'Symbol'],
            '2024-03-07',
            '2024-03-07'
        )

        mock_getHistorical.assert_called_once()
        self.assertEqual(len(dataframes), 3)
        assert_frame_equal(dataframes, mock_historical_dataframe)

    @patch('tradingeconomics.getHistorical')
    @patch('utils.dates.get_year_windows')
    def test_get_comodity_full_historical_many_te_api_call(self, mock_get_year_windows, mock_getHistorical):
        mock_historical_dataframe = get_mock_indicator_historical_dataframe(5)
        mock_get_year_windows.return_value = [
            {"initDate": '1970-01-01', "endDate": '2000-01-01'},
            {"initDate": '2000-01-02', "endDate": '2024-03-07'}
        ]
        mock_getHistorical.side_effect  = [
            mock_historical_dataframe[:5],
            mock_historical_dataframe[5:],
        ]

        dataframe = get_indicator_historical(
            mock_historical_dataframe.loc[0, 'Symbol'],
            '1970-01-01',
            '2024-03-07'
        )

        self.assertEqual(len(dataframe), 5)
        assert_frame_equal(dataframe, mock_historical_dataframe)


    @patch('tradingeconomics.getHistorical')
    @patch('utils.dates.get_year_windows')
    def test_get_comodity_full_historical_data_not_found(self, mock_get_year_windows, mock_getHistorical):
        mock_get_year_windows.return_value = [
            {"initDate": '1900-01-01', "endDate": '1900-01-09'},
        ]
        mock_getHistorical.return_value = None

        dataframe = get_indicator_historical('CO1:COM', '1900-01-01', '1900-01-09')

        self.assertEqual(len(dataframe), 0)
        assert_frame_equal(dataframe, pd.DataFrame())

    @patch('tradingeconomics.getMarketsBySymbol')
    def test_get_indicator_info_not_found(self, mock_getMarketsBySymbol):
        mock_getMarketsBySymbol.return_value = None

        symbol_info = get_indicator_info('Symbol')

        self.assertEqual(symbol_info, None)
        mock_getMarketsBySymbol.assert_called_once_with(symbols = ['Symbol'])
    
    @patch('tradingeconomics.getMarketsBySymbol')
    def test_get_indicator_info_found(self, mock_getMarketsBySymbol):
        expected_indicator_info = get_mock_indicator()
        mock_getMarketsBySymbol.return_value = [expected_indicator_info]

        symbol_info = get_indicator_info(expected_indicator_info['Symbol'])

        self.assertEqual(symbol_info, expected_indicator_info)
        mock_getMarketsBySymbol.assert_called_once_with(symbols = [expected_indicator_info['Symbol']])


    @patch('tradingeconomics.getMarketsBySymbol')
    def test_get_indicators_info_not_found(self, mock_getMarketsBySymbol):
        mock_getMarketsBySymbol.return_value = None

        symbol_info = get_indicators_info(['no-found-symbol'])

        self.assertEqual(symbol_info, None)
        mock_getMarketsBySymbol.assert_called_once_with(symbols = ['no-found-symbol'])

    @patch('tradingeconomics.getMarketsBySymbol')
    def test_get_indicators_info_found(self, mock_getMarketsBySymbol):
        expected_indicators_info = [
            get_mock_indicator(),
            get_mock_indicator()
        ]
        expected_indicators_symbols = [
            expected_indicators_info[0]['Symbol'],
            expected_indicators_info[1]['Symbol']
        ]
        mock_getMarketsBySymbol.return_value = expected_indicators_info
        
        symbol_info = get_indicators_info(expected_indicators_symbols)

        self.assertEqual(symbol_info, expected_indicators_info)
        mock_getMarketsBySymbol.assert_called_once_with(symbols = expected_indicators_symbols)
