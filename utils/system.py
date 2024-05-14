import json
from utils.keyvault import get_secret
from os import environ

DEFAULT_INIT_DATE = environ.get('TRADING_ECONOMICS_HISTORICAL_INIT_DATE', '1964-12-30')

def get_trading_economics_api_key():
    return get_secret('TRADING-ECONOMICS-API-KEY')

def get_trading_economics_indicators_to_request():
    try:
        return json.loads(get_secret('TRADING-ECONOMICS-INDICATORS'))
    except Exception as e:
        return []

def get_filename(**kwargs):
    prefix = kwargs.get('prefix', None)
    name = kwargs['name']
    postfix = kwargs.get('postfix', None)
    ext = kwargs['ext']
    filename = '_'.join(filter(lambda var: var is not None and var != '', [prefix, name, postfix]))
    filename = filename.replace(':', '_')
    return f"{filename}.{ext.replace('.', '')}"
