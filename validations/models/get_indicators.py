from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime, date, timedelta
from utils.system import DEFAULT_INIT_DATE

def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError as e:
        return False

class GetPathParam(BaseModel):
    symbol: str

    @field_validator('symbol')
    @classmethod
    def check_symbol(cls, symbol, _):
        if symbol.isdigit():
            raise ValueError(f'Should be a string')
        return symbol

class GetDailyHeaders(BaseModel):
    indicator_date: Optional[str] = ((date.today()) - timedelta(days=1)).strftime("%Y-%m-%d")

    @field_validator('indicator_date')
    @classmethod
    def check_date_field(cls, date, validation_info):
        if not is_valid_date(date):
            raise ValueError(f'{validation_info.field_name} should be in the following format: YYYY-MM-DD')
        return date

class GetHistoricalHeaders(BaseModel):
    init_date: Optional[str] = DEFAULT_INIT_DATE
    end_date: Optional[str] = ((date.today()) - timedelta(days=1)).strftime("%Y-%m-%d")

    @field_validator('init_date', 'end_date')
    @classmethod
    def check_init_end_date_field(cls, date, validation_info):
        if not is_valid_date(date):
            raise ValueError(f'{validation_info.field_name} should be in the following format: YYYY-MM-DD')
        return date
class GetHistoricalHeadersFred(BaseModel):
    init_date: Optional[str] 
    end_date: Optional[str] = ((date.today()) - timedelta(days=1)).strftime("%Y-%m-%d")

    @field_validator('init_date', 'end_date')
    @classmethod
    def check_init_end_date_field(cls, date, validation_info):
        if not is_valid_date(date):
            raise ValueError(f'{validation_info.field_name} should be in the following format: YYYY-MM-DD')
        return date
