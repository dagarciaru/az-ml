import azure.functions as func
from http_triggers.extract_indicators import blueprint_extract_indicators


app = func.FunctionApp()

app.register_functions(blueprint_extract_indicators)
