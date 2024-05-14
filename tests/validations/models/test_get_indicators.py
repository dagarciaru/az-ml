from unittest import TestCase
from pydantic import ValidationError
from datetime import date, timedelta
from validations.models.get_indicators import GetPathParam, GetHistoricalHeaders

class TestGetIndicators(TestCase):

    def test_GetPathParam_valid_param(self):
        """Test that a valid comodity passes validation."""
        obj = GetPathParam(symbol='XAGUSD:CUR')
        self.assertEqual(obj.symbol, 'XAGUSD:CUR')

    def test_GetPathParam_invalid_param(self):
        self.assertRaisesRegex(
            ValidationError,
            r'.*Should be a string*',
            GetPathParam,
            symbol='123'
        )

    def test_GetHistoricalHeaders_valid_query_params(self):
        params = GetHistoricalHeaders(init_date='2023-01-01', end_date='2023-01-31')
        self.assertEqual(params.init_date, '2023-01-01')
        self.assertEqual(params.end_date, '2023-01-31')

    def test_GetHistoricalHeaders_invalid_query_params(self):
        self.assertRaisesRegex(
            ValidationError,
            r'.*init_date should be in the following format: YYYY-MM-DD*',
            GetHistoricalHeaders,
            init_date='123'
        )
        self.assertRaisesRegex(
            ValidationError,
            r'.*end_date should be in the following format: YYYY-MM-DD*',
            GetHistoricalHeaders,
            end_date='123'
        )

    def test_GetHistoricalHeaders_optional_query_params(self):
        DEFAULT_INIT_DATE = '1964-12-30'
        yesterday_date = ((date.today()) - timedelta(days=1)).strftime("%Y-%m-%d")
        
        params = GetHistoricalHeaders()

        self.assertEqual(params.init_date, DEFAULT_INIT_DATE)
        self.assertEqual(params.end_date, yesterday_date)

        params = GetHistoricalHeaders(init_date='2023-01-01')
        self.assertEqual(params.init_date, '2023-01-01')
        self.assertEqual(params.end_date, yesterday_date)

        params = GetHistoricalHeaders(end_date='2023-01-31')
        self.assertEqual(params.init_date, DEFAULT_INIT_DATE)
        self.assertEqual(params.end_date, '2023-01-31')
