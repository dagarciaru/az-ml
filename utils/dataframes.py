
import io
import pandas as pd
from services.datalake import send_file_to_datalake, get_file_from_datalake
from io import BytesIO

def convert_to_in_memory_parquet(dataframe):
    file_binary = io.BytesIO()
    dataframe.to_parquet(file_binary)
    file_binary.seek(0)
    return file_binary

def get_dataframe(file_name, file_path):
    file = get_file_from_datalake(file_name, file_path)
    if file:
        df = pd.read_parquet(BytesIO(file), engine='pyarrow')
        return df
    dataframe = pd.DataFrame()
    return dataframe

def save_dataframe(dataframe, file_name, file_path = None):
    file_binary = convert_to_in_memory_parquet(dataframe)
    send_file_to_datalake(file_binary, file_name, file_path)
    
def sort_dataframe_by_date(df, reference_column, format='%d/%m/%Y'):
    sort_column = f"{reference_column}_sort"
    df[sort_column] = pd.to_datetime(df[reference_column], format=format)
    dataframe_sorted = df.sort_values(by=sort_column)
    dataframe_sorted.reset_index(drop=True, inplace=True)
    dataframe_sorted.drop(columns=[sort_column], inplace=True)
    return dataframe_sorted

def update_dataframe(dataframe_to_update, dataframe_updates, reference_column, format = None):
    return pd.concat([dataframe_to_update, dataframe_updates]).drop_duplicates([reference_column], keep='last')
