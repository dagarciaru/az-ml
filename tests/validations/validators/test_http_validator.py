from unittest import TestCase
from unittest.mock import MagicMock, Mock
from pydantic import BaseModel
from validations.validators.http_validator import validate, ValidationType
from azure.functions import HttpRequest, HttpResponse

class ModelReurnMock:
    def __init__(self):
        self.key = 'value'
    def __getattr__(self, name):
        if name == '__dict__':
            return self.key

class TestValidateDecorator(TestCase):
    def test_validate_success_query_params(self):
        mock_model = MagicMock(spec=BaseModel)
        req = HttpRequest(params={'key': 'value'}, method='GET', url='localhost', body={})
        def dummy_func(req):
            return HttpResponse(body='Success', status_code=200)
        dummy_func_spy = MagicMock(wraps=dummy_func)

        decorated_dummy_func = validate(model=mock_model, validation_type=ValidationType.QUERY_PARAMS)(dummy_func_spy)
        response = decorated_dummy_func(req)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_body(), b'Success')
        mock_model.assert_called_once_with(key='value')
        dummy_func_spy.assert_called_once_with(req)

    def test_validate_failure_query_params(self):
        class TestModel(BaseModel):
            key: str
        req = HttpRequest(method='GET', url='localhost', params={'key': 1}, body={})

        @validate(model=TestModel, validation_type=ValidationType.QUERY_PARAMS)
        def dummy_func(req):
            pass

        response = dummy_func(req)
        self.assertEqual(response.status_code, 400)
        self.assertTrue(b'"error"' in response.get_body())
        self.assertTrue(b'"msg"' in response.get_body())

    def test_validate_success_body(self):
        mock_model = MagicMock(spec=BaseModel)
        req = HttpRequest(params={}, method='GET', url='localhost', body={"key": "value"})
        def dummy_func(req):
            return HttpResponse(body='Success', status_code=200)
        dummy_func_spy = MagicMock(wraps=dummy_func)

        decorated_dummy_func = validate(model=mock_model, validation_type=ValidationType.BODY)(dummy_func_spy)
        response = decorated_dummy_func(req)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_body(), b'Success')
        mock_model.assert_called_once_with(key='value')
        dummy_func_spy.assert_called_once_with(req)

    def test_validate_failure_body(self):
        class TestModel(BaseModel):
            key: str
        req = HttpRequest(method='GET', url='localhost', body={'key': 1})

        @validate(model=TestModel, validation_type=ValidationType.BODY)
        def dummy_func(req):
            pass

        response = dummy_func(req)
        self.assertEqual(response.status_code, 400)
        self.assertTrue(b'"error"' in response.get_body())
        self.assertTrue(b'"msg"' in response.get_body())

    def test_validate_success_path_params(self):
        mock_model = MagicMock(spec=BaseModel)
        req = HttpRequest(params={}, method='GET', url='localhost', body={}, route_params={'key': 'value'})
        def dummy_func(req):
            return HttpResponse(body='Success', status_code=200)
        dummy_func_spy = MagicMock(wraps=dummy_func)

        decorated_dummy_func = validate(model=mock_model, validation_type=ValidationType.PATH_PARAMS)(dummy_func_spy)
        response = decorated_dummy_func(req)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_body(), b'Success')
        mock_model.assert_called_once_with(key='value')
        dummy_func_spy.assert_called_once_with(req)

    def test_validate_failure_path_params(self):
        class TestModel(BaseModel):
            key: str
        req = HttpRequest(method='GET', url='localhost', route_params={'key': 1}, body={})

        @validate(model=TestModel, validation_type=ValidationType.PATH_PARAMS)
        def dummy_func(req):
            pass

        response = dummy_func(req)
        self.assertEqual(response.status_code, 400)
        self.assertTrue(b'"error"' in response.get_body())
        self.assertTrue(b'"msg"' in response.get_body())

    def test_validate_value_error(self):
        mock_model = MagicMock(spec=BaseModel)
        mock_model.side_effect = ValueError('Error')
        req = HttpRequest(params={}, method='GET', url='localhost', body={"key": "value"})
        def dummy_func(req):
            return HttpResponse(body='Success', status_code=200)
        dummy_func_spy = MagicMock(wraps=dummy_func)

        decorated_dummy_func = validate(model=mock_model, validation_type=ValidationType.BODY)(dummy_func_spy)
        response = decorated_dummy_func(req)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(b'Error', response.get_body())

    def test_validate_insert_path_param_to_func_req(self):
        mock_model = MagicMock()
        mock_model.return_value = ModelReurnMock()
        req = HttpRequest(route_params={'key': 'value'}, method='GET', url='localhost', body={})
        def dummy_func(req):
            return HttpResponse(body='Success', status_code=200)
        dummy_func_spy = MagicMock(wraps=dummy_func)

        decorated_dummy_func = validate(model=mock_model, validation_type=ValidationType.PATH_PARAMS)(dummy_func_spy)
        decorated_dummy_func(req)

        azure_func_req_param = dummy_func_spy.call_args[0][0]

        self.assertEqual(azure_func_req_param.path_params, {'key': 'value'})

    def test_validate_insert_query_param_to_func_req(self):
        mock_model = MagicMock()
        mock_model.return_value = ModelReurnMock()
        req = HttpRequest(params={'key': 'value'}, method='GET', url='localhost', body={})
        def dummy_func(req):
            return HttpResponse(body='Success', status_code=200)
        dummy_func_spy = MagicMock(wraps=dummy_func)

        decorated_dummy_func = validate(model=mock_model, validation_type=ValidationType.QUERY_PARAMS)(dummy_func_spy)
        decorated_dummy_func(req)

        azure_func_req_param = dummy_func_spy.call_args[0][0]

        self.assertEqual(azure_func_req_param.query_params, {'key': 'value'})

    def test_validate_insert_body_to_func_req(self):
        mock_model = MagicMock()
        mock_model.return_value = ModelReurnMock()
        req = HttpRequest(body={'key': 'value'}, method='GET', url='localhost')
        def dummy_func(req):
            return HttpResponse(body='Success', status_code=200)
        dummy_func_spy = MagicMock(wraps=dummy_func)

        decorated_dummy_func = validate(model=mock_model, validation_type=ValidationType.BODY)(dummy_func_spy)
        decorated_dummy_func(req)

        azure_func_req_param = dummy_func_spy.call_args[0][0]

        self.assertEqual(azure_func_req_param.body_params, {'key': 'value'})
