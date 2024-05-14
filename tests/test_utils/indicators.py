import random
import pandas as pd
from datetime import datetime
from faker import Faker

fake = Faker()

def get_mock_indicator():
    return {
        "Symbol": fake.lexify(text="??????"),
        "StartDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "Name": fake.word(),
        "Type": fake.word()
    }

def get_mock_indicator_historical_dataframe(num_rows):
    symbol = fake.lexify(text="??????")
    start_date = datetime(1970, 1, 1)
    end_date = datetime(1970, 12, 31)
    dates = pd.date_range(start_date, end_date, periods=num_rows).strftime('%d/%m/%Y')
    return pd.DataFrame({
        'Symbol': [symbol for _ in range(num_rows)],
        'Date': dates,
        'Open':  [random.uniform(1.0, 10.0) for _ in range(num_rows)],
        'High':  [random.uniform(1.0, 10.0) for _ in range(num_rows)],
        'Low':  [random.uniform(1.0, 10.0) for _ in range(num_rows)],
        'Close':  [random.uniform(1.0, 10.0) for _ in range(num_rows)]
    })
