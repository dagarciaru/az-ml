# import pandas as pd

# def sort_dataframe_by_date(df, reference_column, format='%d/%m/%Y'):
#     sort_column = f"{reference_column}_sort"
#     df[sort_column] = pd.to_datetime(df[reference_column], format=format)
#     dataframe_sorted = df.sort_values(by=sort_column)
#     dataframe_sorted.reset_index(drop=True, inplace=True)
#     dataframe_sorted.drop(columns=[sort_column], inplace=True)
#     return dataframe_sorted

# def update_dataframe(dataframe_to_update, dataframe_updates, reference_column, format = None):
#     return pd.concat([dataframe_to_update, dataframe_updates]).drop_duplicates([reference_column], keep='last')


# base_dataframe = pd.read_parquet('TRADING_ECONOMICS_SPSCFI_COM.parquet')


# dataframe_updates = pd.DataFrame([
#     {"Symbol":"SPSCFI:COM","Date":"12/04/2024","Open":2,"High":2,"Low":2,"Close":2},
#     {"Symbol":"SPSCFI:COM","Date":"10/04/2024","Open":1,"High":1,"Low":1,"Close":1},
#     {"Symbol":"SPSCFI:COM","Date":"15/04/2024","Open":3,"High":3,"Low":3,"Close":3},
#     {"Symbol":"SPSCFI:COM","Date":"06/09/2013","Open":4,"High":4,"Low":4,"Close":4}
# ])


# new_df = update_dataframe(base_dataframe, dataframe_updates, 'Date')
# sorted_df = sort_dataframe_by_date(new_df, 'Date')

# print(new_df.info())

# # print(dataframe_updates)
# # print(base_dataframe)

# sorted_df.to_parquet('output.parquet')



import requests
try:
    api_key = 'A250DAAD00EA4D1:45D62E3CC47C488'
    url = f'https://api.tradingeconomics.com/country/mexico?c={api_key}'
    data = requests.get(url)
    print(data)

except Exception as e:
    print(e)
# The response data format can be configured by appending the &f= parameter to the URL request (supported formats: JSON, CSV, XML)

                     
# url = f'https://api.tradingeconomics.com/country/mexico?c={api_key}&f=xml'
# You can use request headers to pass the API key:

                     
# import requests

# response = requests.get('https://api.tradingeconomics.com/country/mexico', headers = {'Authorization': 'api_key'})
# print(response.json())