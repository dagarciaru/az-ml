from unittest import TestCase
from unittest.mock import patch, MagicMock
import json

class TestUtils(TestCase):

    @patch("azure.keyvault.secrets.SecretClient", MagicMock())
    def setUp(self):
        global get_trading_economics_api_key
        global get_trading_economics_indicators_to_request
        global get_filename
        from utils.system import get_trading_economics_api_key, get_trading_economics_indicators_to_request, get_filename
        
    @patch('utils.system.get_secret')
    def test_get_trading_economics_api_key(self, mock_get_secret):
        mock_get_secret.return_value = 'api-key'
        
        key = get_trading_economics_api_key()

        self.assertEqual(key, 'api-key')

    @patch('utils.system.get_secret')
    def test_get_trading_economics_indicators_to_request(self, mock_get_secret):
        indicators_from_secret = "[\"CO1:COM\"]"
        expected_comodities = json.loads(indicators_from_secret)

        mock_get_secret.return_value = indicators_from_secret

        comodities = get_trading_economics_indicators_to_request()

        self.assertEqual(comodities, expected_comodities)

    def test_get_parquet_file_name_all_vars_present(self):
        file_name = get_filename(
            prefix='prefix',
            name='XAGUSD:CUR',
            postfix='postfix',
            ext='parquet'
            )
        self.assertEqual(file_name, 'prefix_XAGUSD_CUR_postfix.parquet')
