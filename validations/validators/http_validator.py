from typing import Callable
from azure.functions import HttpRequest, HttpResponse
import functools
from pydantic import BaseModel, ValidationError
import json
from enum import Enum
import logging 
from utils.exceptions import IndicatorException

class ValidationType(Enum):
    BODY = 1
    PATH_PARAMS = 2
    QUERY_PARAMS = 3
    HEADERS = 4

def format_error_outputs(validation_error: ValidationError):
    errors = []
    for error in validation_error.errors():
        logging.info(error)
        errors.append({"error": error['type'], "msg": f"{error['msg']}: {error['loc'][0]}"})
    return errors

def validate(model: BaseModel, validation_type: ValidationType):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(req: HttpRequest, *args, **kwargs):
            try:
                if validation_type == ValidationType.QUERY_PARAMS:
                    params = req.params
                    parsed_params = model(**params)
                    setattr(req, 'query_params', parsed_params.__dict__)
                elif validation_type == ValidationType.PATH_PARAMS:
                    params = req.route_params
                    parsed_params = model(**params)
                    setattr(req, 'path_params', parsed_params.__dict__)
                elif validation_type == ValidationType.BODY:
                    params = req.get_body()
                    parsed_params = model(**params)
                    setattr(req, 'body_params', parsed_params.__dict__)
                elif validation_type == ValidationType.HEADERS:
                    params = req.headers.__dict__["__http_headers__"]
                    parsed_params = model(**params)
                    setattr(req, 'header_params', parsed_params.__dict__)
                return func(req, *args, **kwargs)
            except ValidationError as e:
                output = json.dumps(format_error_outputs(e))
                return HttpResponse(body=output, status_code=400)
            except ValueError as e:
                return HttpResponse(body=str(e), status_code=400)
            except IndicatorException as e:
                return HttpResponse(body=e.message, status_code=e.status_code)
            except Exception as e:
                logging.info(str(e))
                return HttpResponse(body='Internal Error', status_code=500)
        return wrapper
    return decorator
