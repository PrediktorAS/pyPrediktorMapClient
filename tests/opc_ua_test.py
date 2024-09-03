import unittest
from unittest import mock
from unittest.mock import patch
from requests.exceptions import HTTPError
import pytest
from datetime import datetime, timedelta, date
import aiohttp
import pandas.api.types as ptypes
from pydantic import ValidationError, AnyUrl, BaseModel
from requests.exceptions import HTTPError
from typing import List
from copy import deepcopy
from pydantic_core import Url
import asyncio
import requests
import json
import logging
import pandas as pd
from parameterized import parameterized
from aiohttp.client_exceptions import ClientResponseError, ClientError
from yarl import URL as YarlURL

from pyprediktormapclient.opc_ua import OPC_UA, Variables, WriteVariables, WriteHistoricalVariables, Value
from pyprediktormapclient.auth_client import AUTH_CLIENT, Token

URL = "http://someserver.somedomain.com/v1/"
OPC_URL = "opc.tcp://nosuchserver.nosuchdomain.com"
username = "some@user.com"
password = "somepassword"
auth_id = "0b518533-fb09-4bb7-a51f-166d3453685e"
auth_session_id = "qlZULxcaNc6xVdXQfqPxwix5v3tuCLaO"
auth_expires_at = "2022-12-04T07:31:28.767407252Z"

list_of_ids = [
    {"Id": "SOMEID1", "Namespace": "1", "IdType": "2"},
]

list_of_write_values = [
    {
        "NodeId": {"Id": "SOMEID", "Namespace": 1, "IdType": 2},
        "Value": {
            "Value": {"Type": 10, "Body": 1.2},
            "SourceTimestamp": "2022-11-03T12:00:00Z",
            "StatusCode": {"Code": 0, "Symbol": "Good"},
        },
    }
]

list_of_write_historical_values = [
    {
        "NodeId": {"Id": "SOMEID", "Namespace": 1, "IdType": 2},
        "PerformInsertReplace": 1,
        "UpdateValues": [
            {
                "Value": {"Type": 10, "Body": 1.1},
                "SourceTimestamp": "2022-11-03T12:00:00Z",
                "StatusCode": {"Code": 0, "Symbol": "Good"},
            },
            {
                "Value": {"Type": 10, "Body": 2.1},
                "SourceTimestamp": "2022-11-03T13:00:00Z",
                "StatusCode": {"Code": 0, "Symbol": "Good"},
            },
        ],
    }
]

list_of_write_historical_values_in_wrong_order = [
    {
        "NodeId": {"Id": "SOMEID", "Namespace": 4, "IdType": 1},
        "PerformInsertReplace": 1,
        "UpdateValues": [
            {
                "Value": {"Type": 10, "Body": 1.1},
                "SourceTimestamp": "2022-11-03T14:00:00Z",
                "StatusCode": {"Code": 0, "Symbol": "Good"},
            },
            {
                "Value": {"Type": 10, "Body": 2.1},
                "SourceTimestamp": "2022-11-03T13:00:00Z",
                "StatusCode": {"Code": 0, "Symbol": "Good"},
            },
        ],
    }
]

list_of_historical_values_wrong_type_and_value = [
    {
        "NodeId": {"Id": "SSO.JO-GL.321321321", "Namespace": 4, "IdType": 1},
        "PerformInsertReplace": 1,
        "UpdateValues": [
            {
                "Value": {"Type": 10, "Body": 1.1},
                "SourceTimestamp": "2022-11-03T14:00:00Z",
                "StatusCode": {"Code": 0, "Symbol": "Good"},
            },
            {
                "Value": {"Type": 1, "Body": "2.1"},
                "SourceTimestamp": "2022-11-03T15:00:00Z",
                "StatusCode": {"Code": 0, "Symbol": "Good"},
            },
        ],
    }
]

successful_live_response = [
    {
        "Success": True,
        "Values": [
            {
                "NodeId": {"Id": "SOMEID", "Namespace": 0, "IdType": 0},
                "Value": {"Type": 10, "Body": 1.2},
                "ServerTimestamp": "2022-01-01T12:00:00Z",
                "StatusCode": {"Code": 0, "Symbol": "Good"},
            },
            {
                "NodeId": {"Id": "SOMEID2", "Namespace": 0, "IdType": 0},
                "Value": {"Type": 11, "Body": 2.3},
                "ServerTimestamp": "2022-01-01T12:05:00Z",
                "StatusCode": {"Code": 1, "Symbol": "Uncertain"},
            },
        ],
    }
]

empty_live_response = [
    {
        "Success": True,
        "Values": [
            {
                "SourceTimestamp": "2022-09-21T13:13:38.183Z",
                "ServerTimestamp": "2022-09-21T13:13:38.183Z",
            },
            {
                "SourceTimestamp": "2023-09-21T13:13:38.183Z",
                "ServerTimestamp": "2023-09-21T13:13:38.183Z",
            },
        ],
    }
]

successful_historical_result = {
    "Success": True,
    "ErrorMessage": "",
    "ErrorCode": 0,
    "ServerNamespaces": ["string"],
    "HistoryReadResults": [
        {
            "NodeId": {
                "IdType": 2,
                "Id": "SOMEID",
                "Namespace": 1,
            },
            "StatusCode": {"Code": 0, "Symbol": "Good"},
            "DataValues": [
                {
                    "Value": {"Type": 11, "Body": 34.28500000000003},
                    "StatusCode": {"Code": 1, "Symbol": "Good"},
                    "SourceTimestamp": "2022-09-13T13:39:51Z",
                },
                {
                    "Value": {"Type": 11, "Body": 6.441666666666666},
                    "StatusCode": {"Code": 1, "Symbol": "Good"},
                    "SourceTimestamp": "2022-09-13T14:39:51Z",
                },
            ],
        },
        {
            "NodeId": {
                "IdType": 2,
                "Id": "SOMEID2",
                "Namespace": 1,
            },
            "StatusCode": {"Code": 0, "Symbol": "Good"},
            "DataValues": [
                {
                    "Value": {"Type": 11, "Body": 34.28500000000003},
                    "StatusCode": {"Code": 1, "Symbol": "Good"},
                    "SourceTimestamp": "2022-09-13T13:39:51Z",
                },
                {
                    "Value": {"Type": 11, "Body": 6.441666666666666},
                    "StatusCode": {"Code": 1, "Symbol": "Good"},
                    "SourceTimestamp": "2022-09-13T14:39:51Z",
                },
            ],
        },
    ],
}

successful_raw_historical_result = {
    "Success": True,
    "ErrorMessage": "",
    "ErrorCode": 0,
    "ServerNamespaces": ["string"],
    "HistoryReadResults": [
        {
            "NodeId": {
                "IdType": 2,
                "Id": "SOMEID",
                "Namespace": 1,
            },
            "StatusCode": {"Code": 0, "Symbol": "Good"},
            "DataValues": [
                {
                    "Value": {"Type": 11, "Body": 34.28500000000003},
                    "SourceTimestamp": "2022-09-13T13:39:51Z",
                },
                {
                    "Value": {"Type": 11, "Body": 35.12345678901234},
                    "SourceTimestamp": "2022-09-13T13:40:51Z",
                },
                {
                    "Value": {"Type": 11, "Body": 33.98765432109876},
                    "SourceTimestamp": "2022-09-13T13:41:51Z",
                },
            ],
        },
        {
            "NodeId": {
                "IdType": 2,
                "Id": "SOMEID2",
                "Namespace": 1,
            },
            "StatusCode": {"Code": 0, "Symbol": "Good"},
            "DataValues": [
                {
                    "Value": {"Type": 11, "Body": 6.441666666666666},
                    "SourceTimestamp": "2022-09-13T13:39:51Z",
                },
                {
                    "Value": {"Type": 11, "Body": 6.523456789012345},
                    "SourceTimestamp": "2022-09-13T13:40:51Z",
                },
                {
                    "Value": {"Type": 11, "Body": 6.345678901234567},
                    "SourceTimestamp": "2022-09-13T13:41:51Z",
                },
            ],
        },
    ],
}

successful_write_live_response = {
    "Success": True,
    "ErrorMessage": "string",
    "ErrorCode": 0,
    "ServerNamespaces": ["string"],
    "StatusCodes": [{"Code": 0, "Symbol": "Good"}],
}


empty_write_live_response = {
    "Success": True,
    "ErrorMessage": "string",
    "ErrorCode": 0,
    "ServerNamespaces": ["string"],
    "StatusCodes": [{}],
}

successful_write_historical_response = {
    "Success": True,
    "ErrorMessage": "string",
    "ErrorCode": 0,
    "ServerNamespaces": ["string"],
    "HistoryUpdateResults": [{}],
}

unsuccessful_write_historical_response = {
    "Success": True,
    "ErrorMessage": "string",
    "ErrorCode": 0,
    "ServerNamespaces": ["string"],
}

successfull_write_historical_response_with_errors = {
    "Success": True,
    "ErrorMessage": "string",
    "ErrorCode": 0,
    "ServerNamespaces": ["string"],
    "HistoryUpdateResults": [
        {"StatusCode": {"Code": 2158690304, "Symbol": "BadInvalidArgument"}}
    ],
}


class SubValue(BaseModel):
    Type: int
    Body: float


class StatusCode(BaseModel):
    Code: int
    Symbol: str


class Value(BaseModel):
    Value: SubValue
    SourceTimestamp: str
    StatusCode: StatusCode


class Variables(BaseModel):
    Id: str
    Namespace: int
    IdType: int


class WriteHistoricalVariables(BaseModel):
    NodeId: Variables
    PerformInsertReplace: int
    UpdateValues: List[Value]


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code
        self.raise_for_status = mock.Mock(return_value=False)
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return self.json_data


# This method will be used by the mock to replace requests
def successful_mocked_requests(*args, **kwargs):
    status_code = 200 if args[0] == f"{URL}values/get" else 404
    json_data = successful_live_response
    response = MockResponse(json_data=json_data, status_code=status_code)
    response.json_data = json_data
    return response


def empty_values_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/get":
        return MockResponse(empty_live_response, 200)

    return MockResponse(None, 404)


def successful_write_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/set":
        return MockResponse(successful_write_live_response, 200)

    return MockResponse(None, 404)


def empty_write_values_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/set":
        return MockResponse(empty_write_live_response, 200)

    return MockResponse(None, 404)


def no_write_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/set":
        # Set Success to False
        response = ""
        return MockResponse(response, 200)

    return MockResponse(None, 404)


def unsuccessful_write_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/set":
        # Set Success to False
        unsuc = deepcopy(successful_write_live_response)
        unsuc["Success"] = False
        return MockResponse(unsuc, 200)

    return MockResponse(None, 404)


def no_status_code_write_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/set":
        # Set Success to False
        nostats = deepcopy(successful_write_live_response)
        nostats["StatusCodes"][0].pop("Code")
        return MockResponse(nostats, 200)

    return MockResponse(None, 404)


def empty_write_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/set":
        # Remove values from the dict
        empty = deepcopy(successful_write_live_response)
        empty.pop("StatusCodes")
        return MockResponse(empty, 200)

    return MockResponse(None, 404)


def successful_write_historical_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalwrite":
        return MockResponse(successful_write_historical_response, 200)

    return MockResponse(None, 404)


def no_write_mocked_historical_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalwrite":
        # Set Success to False
        response = ""
        return MockResponse(response, 200)

    return MockResponse(None, 404)


def successful_write_historical_with_errors_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalwrite":
        suce = deepcopy(successfull_write_historical_response_with_errors)
        return MockResponse(suce, 200)

    return MockResponse(None, 404)


def unsuccessful_write_historical_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalwrite":
        # Set Success to False
        unsuc = deepcopy(successful_write_historical_response)
        unsuc["Success"] = False
        return MockResponse(unsuc, 200)

    return MockResponse(None, 404)


def no_status_code_write_historical_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalwrite":
        # Set Success to False
        nostats = deepcopy(successful_write_historical_response)
        nostats["StatusCodes"][0].pop("Code")
        return MockResponse(nostats, 200)

    return MockResponse(None, 404)


def empty_write_historical_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalwrite":
        # Remove values from the dict
        empty = deepcopy(successful_write_historical_response)
        empty.pop("HistoryUpdateResults")
        return MockResponse(empty, 200)

    return MockResponse(None, 404)


def no_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/get":
        # Set Success to False
        response = ""
        return MockResponse(response, 200)

    return MockResponse(None, 404)


def unsuccessful_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/get":
        # Set Success to False
        unsuc = deepcopy(successful_live_response[0])
        unsuc["Success"] = False
        return MockResponse([unsuc], 200)

    return MockResponse(None, 404)


def no_status_code_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/get":
        # Set Success to False
        nostats = deepcopy(successful_live_response[0])
        nostats["Values"][0].pop("StatusCode")
        return MockResponse([nostats], 200)

    return MockResponse(None, 404)


def empty_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/get":
        # Remove values from the dict
        empty = deepcopy(successful_live_response[0])
        empty.pop("Values")
        return MockResponse([empty], 200)

    return MockResponse(None, 404)


class AnyUrlModel(BaseModel):
    url: AnyUrl


class TestCaseOPCUA(unittest.TestCase):

    def test_rest_url_ends_with_slash(self):
        url_with_trailing_slash = URL.rstrip("/") + "/"
        opc = OPC_UA(rest_url=url_with_trailing_slash, opcua_url=OPC_URL)
        assert opc.rest_url == URL

    def test_invalid_opcua_url(self):
        with pytest.raises(ValueError, match="Invalid OPC UA URL"):
            OPC_UA(rest_url=URL, opcua_url="http://invalidurl.com")

    def test_json_serial(self):
        logging.basicConfig(level=logging.DEBUG)
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
    
        dt = datetime(2024, 9, 2, 12, 0, 0)
        assert opc.json_serial(dt) == "2024-09-02T12:00:00"
        
        d = date(2024, 9, 2)
        assert opc.json_serial(d) == "2024-09-02"
    
        url = Url("http://example.com")
        result = opc.json_serial(url)
        assert result == "http://example.com/"
        assert isinstance(result, str)
    
        with pytest.raises(TypeError) as excinfo:
            opc.json_serial(set())
        assert "Type <class 'set'> not serializable" in str(excinfo.value)

    def test_check_auth_client(self):
        auth_client_mock = mock.Mock()
        auth_client_mock.token.session_token = "test_token"
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client_mock)
    
        content = {"error": {"code": 404}}
        opc.check_auth_client(content)
        auth_client_mock.request_new_ory_token.assert_called_once()
        assert opc.headers["Authorization"] == "Bearer test_token"
        
        auth_client_mock.reset_mock()

        content = {"error": {"code": 500}, "ErrorMessage": "Server Error"}
        with pytest.raises(RuntimeError) as excinfo:
            opc.check_auth_client(content)
        assert str(excinfo.value) == "Server Error"
        auth_client_mock.request_new_ory_token.assert_not_called()

        content = {"error": {}}
        with pytest.raises(RuntimeError):
            opc.check_auth_client(content)
        auth_client_mock.request_new_ory_token.assert_not_called()

    def test_check_if_ory_session_token_is_valid_refresh(self):
        auth_client_mock = mock.Mock()
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client_mock)
       
        auth_client_mock.check_if_token_has_expired.return_value = True
        opc.check_if_ory_session_token_is_valid_refresh()
        auth_client_mock.check_if_token_has_expired.assert_called_once()
        auth_client_mock.refresh_token.assert_called_once()
      
        auth_client_mock.reset_mock()
      
        auth_client_mock.check_if_token_has_expired.return_value = False
        opc.check_if_ory_session_token_is_valid_refresh()
        auth_client_mock.check_if_token_has_expired.assert_called_once()
        auth_client_mock.refresh_token.assert_not_called()

    @mock.patch('pyprediktormapclient.opc_ua.request_from_api')
    def test_get_values_request_error(self, mock_request):
        mock_request.side_effect = Exception("Test exception")
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with self.assertRaises(RuntimeError) as context:
            opc.get_values(list_of_ids)
        self.assertEqual(str(context.exception), "Error in get_values: Test exception")

    @patch('requests.post')
    @patch('pyprediktormapclient.shared.request_from_api')
    def test_get_values_with_auth_client(self, mock_request_from_api, mock_post):
        auth_client_mock = mock.Mock()
        auth_client_mock.token.session_token = "test_token"
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client_mock)
        
        mock_response = mock.Mock()
        mock_response.content = json.dumps({"error": {"code": 404}}).encode()
        mock_request_from_api.side_effect = [
            requests.exceptions.HTTPError("404 Client Error", response=mock_response),
            successful_live_response
        ]
        mock_post.return_value = MockResponse(successful_live_response, 200)

        result = opc.get_values(list_of_ids)
        
        self.assertIsNotNone(result)
        self.assertTrue(mock_request_from_api.call_count > 0 or mock_post.call_count > 0, 
                        "Neither request_from_api nor post was called")

        self.assertEqual(len(result), len(list_of_ids))
        for i, item in enumerate(result):
            self.assertEqual(item['Id'], list_of_ids[i]['Id'])

        if opc.headers:
            self.assertIn("Authorization", opc.headers, "Authorization header is missing")
            self.assertIn("test_token", opc.headers.get("Authorization", ""), 
                          "Session token not found in Authorization header")

    def test_malformed_rest_url(self):
        with pytest.raises(ValidationError):
            AnyUrlModel(rest_url="not_an_url", opcua_url=OPC_URL)

    def test_malformed_opcua_url(self):
        with pytest.raises(ValidationError):
            AnyUrlModel(rest_url=URL, opcua_url="not_an_url")

    def test_namespaces(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, namespaces=["1", "2"])
        assert "ClientNamespaces" in opc.body
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        assert "ClientNamespaces" not in opc.body

    def test_get_value_type(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result_none = opc._get_value_type(100000)
        assert result_none["id"] is None
        result = opc._get_value_type(1)
        assert "id" in result
        assert "type" in result
        assert "description" in result
        assert result["type"] == "Boolean"

    def test_get_value_type_not_found(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = opc._get_value_type(100000)
        assert result["id"] is None
        assert result["type"] is None
        assert result["description"] is None

    def test_get_variable_list_as_list(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)

        pydantic_var = Variables(Id="SOMEID", Namespace=1, IdType=2)
        result = opc._get_variable_list_as_list([pydantic_var])
        assert isinstance(result[0], dict)
        assert result[0] == {"Id": "SOMEID", "Namespace": 1, "IdType": 2}

        dict_var = {"Id": "SOMEID2", "Namespace": 2, "IdType": 1}
        result = opc._get_variable_list_as_list([dict_var])
        assert isinstance(result[0], dict)
        assert result[0] == dict_var

        with pytest.raises(TypeError, match="Unsupported type in variable_list"):
            opc._get_variable_list_as_list([123])

    def test_get_values_variable_list_not_list(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        not_a_list = "not_a_list"

        with pytest.raises(TypeError, match="Unsupported type in variable_list"):
            opc.get_values(not_a_list)

    def test_get_variable_list_as_list_invalid_type(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with self.assertRaises(TypeError):
            opc._get_variable_list_as_list([1, 2, 3])

    def test_check_auth_client_is_none(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, auth_client=None)
        with pytest.raises(Exception):
            opc.check_auth_client()

    @parameterized.expand([
        (None, None, None),  # Without auth client
        (AUTH_CLIENT, username, password)  # With auth client
    ])
    @mock.patch("requests.post", side_effect=successful_mocked_requests)
    def test_get_live_values_successful(self, AuthClientClass, username, password, mock_get):
        if AuthClientClass:
            auth_client = AuthClientClass(
                rest_url=URL, username=username, password=password
            )
            auth_client.token = Token(
                session_token=auth_session_id, expires_at=auth_expires_at
            )
            tsdata = OPC_UA(
                rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client
            )
        else:
            tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)

        result = tsdata.get_values(list_of_ids)
        for num, row in enumerate(list_of_ids):
            assert result[num]["Id"] == list_of_ids[num]["Id"]
            assert (
                result[num]["Timestamp"]
                == successful_live_response[0]["Values"][num]["ServerTimestamp"]
            )
            assert (
                result[num]["Value"]
                == successful_live_response[0]["Values"][num]["Value"]["Body"]
            )
            assert (
                result[num]["ValueType"]
                == tsdata._get_value_type(
                    successful_live_response[0]["Values"][num]["Value"]["Type"]
                )["type"]
            )

    @mock.patch("requests.post", side_effect=empty_values_mocked_requests)
    def test_get_live_values_with_missing_value_and_statuscode(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.get_values(list_of_ids)
        for num, row in enumerate(list_of_ids):
            if num < len(result):
                assert result[num]["Id"] == list_of_ids[num]["Id"]
                assert (
                    result[num]["Timestamp"]
                    == empty_live_response[0]["Values"][num]["ServerTimestamp"]
                )
                assert result[num]["Value"] is None
                assert result[num]["ValueType"] is None
                assert result[num]["StatusCode"] is None
                assert result[num]["StatusSymbol"] is None

    @mock.patch("requests.post", side_effect=no_mocked_requests)
    def test_get_live_values_no_response(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.get_values(list_of_ids)
        assert result[0]["Timestamp"] is None

    @mock.patch("requests.post", side_effect=unsuccessful_mocked_requests)
    def test_get_live_values_unsuccessful(self, mock_post):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with pytest.raises(RuntimeError):
            tsdata.get_values(list_of_ids)

    @mock.patch("requests.post", side_effect=empty_mocked_requests)
    def test_get_live_values_empty(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.get_values(list_of_ids)
        assert result[0]["Timestamp"] is None

    @mock.patch("requests.post", side_effect=no_status_code_mocked_requests)
    def test_get_live_values_no_status_code(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.get_values(list_of_ids)
        assert result[0]["StatusCode"] is None

    @mock.patch('pyprediktormapclient.opc_ua.request_from_api')
    def test_get_values_error_handling(self, mock_request):
        mock_request.side_effect = Exception("Test exception")
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with self.assertRaises(RuntimeError) as context:
            opc.get_values(list_of_ids)
        self.assertEqual(str(context.exception), "Error in get_values: Test exception")

    @mock.patch('pyprediktormapclient.opc_ua.request_from_api')
    def test_write_values_error_handling(self, mock_request):
        mock_request.side_effect = Exception("Test exception")
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with self.assertRaises(RuntimeError) as context:
            opc.write_values(list_of_write_values)
        self.assertEqual(str(context.exception), "Error in write_values: Test exception")

    @mock.patch('pyprediktormapclient.opc_ua.request_from_api')
    def test_write_values_http_error_handling(self, mock_request):
        auth_client_mock = mock.Mock()
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client_mock)
        
        mock_response = mock.Mock()
        mock_response.content = json.dumps({"error": {"code": 404}}).encode()
        http_error = requests.exceptions.HTTPError("404 Client Error", response=mock_response)
        mock_request.side_effect = [http_error, {"Success": True, "StatusCodes": [{"Code": 0}]}]
        
        opc.check_auth_client = mock.Mock()
        
        result = opc.write_values(list_of_write_values)
        
        opc.check_auth_client.assert_called_once_with({"error": {"code": 404}})
        self.assertIsNotNone(result)
        self.assertEqual(len(result), len(list_of_write_values))

    @mock.patch('pyprediktormapclient.opc_ua.request_from_api')
    def test_write_historical_values_http_error_handling(self, mock_request):
        auth_client_mock = mock.Mock()
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client_mock)
        
        mock_response = mock.Mock()
        mock_response.content = json.dumps({"error": {"code": 404}}).encode()
        http_error = requests.exceptions.HTTPError("404 Client Error", response=mock_response)
        mock_request.side_effect = [http_error, {"Success": True, "HistoryUpdateResults": [{}]}]
        
        opc.check_auth_client = mock.Mock()
        
        converted_data = [WriteHistoricalVariables(**item) for item in list_of_write_historical_values]
        result = opc.write_historical_values(converted_data)
        
        opc.check_auth_client.assert_called_once_with({"error": {"code": 404}})
        self.assertIsNotNone(result)
        self.assertEqual(len(result), len(converted_data))

    @mock.patch("requests.post", side_effect=successful_write_mocked_requests)
    def test_write_live_values_successful(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.write_values(list_of_write_values)
        for num, row in enumerate(list_of_write_values):
            assert (
                result[num]["Value"]["StatusCode"]["Code"]
                == successful_write_live_response["StatusCodes"][num]["Code"]
            )
            assert (
                result[num]["Value"]["StatusCode"]["Symbol"]
                == successful_write_live_response["StatusCodes"][num]["Symbol"]
            )
            assert result[num]["WriteSuccess"] is True

    @mock.patch(
        "requests.post", side_effect=empty_write_values_mocked_requests
    )
    def test_write_live_values_with_missing_value_and_statuscode(
        self, mock_get
    ):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.write_values(list_of_write_values)
        for num, row in enumerate(list_of_write_values):
            assert result[num]["WriteSuccess"] is False

    @mock.patch("requests.post", side_effect=no_write_mocked_requests)
    def test_get_write_live_values_no_response(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.write_values(list_of_write_values)
        assert result is None

    @mock.patch(
        "requests.post", side_effect=unsuccessful_write_mocked_requests
    )
    def test_get_write_live_values_unsuccessful(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with pytest.raises(RuntimeError):
            tsdata.write_values(list_of_write_values)

    @mock.patch("requests.post", side_effect=empty_write_mocked_requests)
    def test_get_write_live_values_empty(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with pytest.raises(ValueError):
            tsdata.write_values(list_of_write_values)

    @mock.patch("requests.post")
    def test_get_values_no_content(self, mock_request):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = opc.get_values(list_of_ids)
        assert result[0]["Timestamp"] is None

    def test_process_content(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
    
        content = {
            "Success": True,
            "HistoryReadResults": [
                {
                    "NodeId": {"Id": "SOMEID", "Namespace": 1, "IdType": 2},
                    "DataValues": [
                        {"Value": {"Type": 11, "Body": 1.23}, "SourceTimestamp": "2023-01-01T00:00:00Z"}
                    ]
                }
            ]
        }
        result = opc._process_content(content)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["Value.Body"] == 1.23
        
        with pytest.raises(RuntimeError, match="No content returned from the server"):
            opc._process_content(None)

        content = {"Success": False, "ErrorMessage": "Error"}
        with pytest.raises(RuntimeError, match="Error"):
            opc._process_content(content)

        content = {"Success": True}
        with pytest.raises(RuntimeError, match="No history read results returned from the server"):
            opc._process_content(content)

    @mock.patch("requests.post")
    def test_get_values_unsuccessful(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
       
        error_response = mock.Mock()
        error_response.status_code = 500
        error_response.raise_for_status.side_effect = HTTPError("500 Server Error: Internal Server Error for url")
       
        mock_post.return_value = error_response
        with pytest.raises(RuntimeError):
            opc.get_values(list_of_ids)

    @parameterized.expand([
        (WriteVariables, list_of_write_values, "write_values"),
        (WriteHistoricalVariables, list_of_write_historical_values, "write_historical_values")
    ])
    @mock.patch("requests.post")
    def test_write_no_content(self, VariableClass, list_of_values, write_method, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)

        no_content_response = mock.Mock()
        no_content_response.status_code = 200
        no_content_response.json.return_value = {"Success": True}
        no_content_response.headers = {"Content-Type": "application/json"}

        mock_post.return_value = no_content_response
        converted_data = [VariableClass(**item) for item in list_of_values]

        with pytest.raises(ValueError, match="No status codes returned, might indicate no values written"):
            getattr(opc, write_method)(converted_data)

    @parameterized.expand([
        (WriteVariables, list_of_write_values, "write_values", 500),
        (WriteHistoricalVariables, list_of_write_historical_values, "write_historical_values", 200)
    ])
    @mock.patch("requests.post")
    def test_write_unsuccessful(self, VariableClass, list_of_values, write_method, status_code, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)

        error_response = mock.Mock()
        error_response.status_code = status_code
        error_response.json.return_value = {"Success": False, "ErrorMessage": "Error"}
        error_response.headers = {"Content-Type": "application/json"}

        mock_post.return_value = error_response
        converted_data = [VariableClass(**item) for item in list_of_values]

        with pytest.raises(RuntimeError, match="Error"):
            getattr(opc, write_method)(converted_data)

    @mock.patch("requests.post")
    def test_write_values_no_status_codes(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
       
        error_response = mock.Mock()
        error_response.status_code = 200
        error_response.json.return_value = {"Success": True}
        error_response.headers = {"Content-Type": "application/json"}
       
        mock_post.return_value = error_response
        with pytest.raises(ValueError):
            opc.write_values(list_of_write_values)

    @mock.patch(
        "requests.post",
        side_effect=successful_write_historical_mocked_requests,
    )
    def test_write_historical_values_successful(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_write_historical_values
        ]
        result = tsdata.write_historical_values(converted_data)
        for num, row in enumerate(list_of_write_values):
            assert result[0]["WriteSuccess"] is True

    @mock.patch(
        "requests.post",
        side_effect=successful_write_historical_mocked_requests,
    )
    def test_write_wrong_order_historical_values_successful(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_write_historical_values_in_wrong_order
        ]
        with pytest.raises(ValueError):
            tsdata.write_historical_values(converted_data)

    @mock.patch(
        "requests.post", side_effect=empty_write_historical_mocked_requests
    )
    def test_write_historical_values_with_missing_value_and_statuscode(
        self, mock_get
    ):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_write_historical_values
        ]
        with pytest.raises(ValueError):
            tsdata.write_historical_values(converted_data)

    @mock.patch(
        "requests.post", side_effect=no_write_mocked_historical_requests
    )
    def test_get_write_historical_values_no_response(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_write_historical_values
        ]
        result = tsdata.write_historical_values(converted_data)
        assert result is None

    @mock.patch(
        "requests.post",
        side_effect=unsuccessful_write_historical_mocked_requests,
    )
    def test_get_write_historical_values_unsuccessful(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_write_historical_values
        ]
        with pytest.raises(RuntimeError):
            tsdata.write_historical_values(converted_data)

    @mock.patch(
        "requests.post",
        side_effect=successful_write_historical_with_errors_mocked_requests,
    )
    def test_get_write_historical_values_successful_with_error_codes(
        self, mock_get
    ):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_historical_values_wrong_type_and_value
        ]
        result = tsdata.write_historical_values(converted_data)
        assert (
            result[0]["WriteError"]["Code"]
            == successfull_write_historical_response_with_errors[
                "HistoryUpdateResults"
            ][0]["StatusCode"]["Code"]
        )
        assert (
            result[0]["WriteError"]["Symbol"]
            == successfull_write_historical_response_with_errors[
                "HistoryUpdateResults"
            ][0]["StatusCode"]["Symbol"]
        )

    @mock.patch("requests.post")
    def test_write_historical_values_no_history_update_results(self, mock_request):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
      
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_write_historical_values
        ]
        with pytest.raises(ValueError, match="No status codes returned, might indicate no values written"):
            opc.write_historical_values(converted_data)

    @mock.patch("requests.post")
    def test_write_historical_values_wrong_order(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
       
        wrong_order_response = mock.Mock()
        wrong_order_response.status_code = 200
        wrong_order_response.json.return_value = {"Success": False, "ErrorMessage": "UpdateValues attribute missing"}
        wrong_order_response.headers = {"Content-Type": "application/json"}
      
        mock_post.return_value = wrong_order_response
        converted_data = [
            WriteHistoricalVariables(**item)
            for item in list_of_write_historical_values_in_wrong_order
        ]
        
        with pytest.raises(ValueError, match="Time for variables not in correct order."):
            opc.write_historical_values(converted_data)


class AsyncMockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status = status_code
        self.headers = {"Content-Type": "application/json"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def json(self):
        return self.json_data

    async def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=aiohttp.RequestInfo(
                    url=URL, method="POST", headers={}, real_url=OPC_URL
                ),
                history=(),
                status=self.status,
                message="Mocked error",
                headers=self.headers,
            )


def unsuccessful_async_mock_response(*args, **kwargs):
    return AsyncMockResponse(json_data=None, status_code=400)


async def make_historical_request():
    tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
    return await tsdata.get_historical_aggregated_values_asyn(
        start_time=(datetime.now() - timedelta(30)),
        end_time=(datetime.now() - timedelta(29)),
        pro_interval=3600000,
        agg_name="Average",
        variable_list=list_of_ids,
    )


async def make_raw_historical_request():
    tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
    return await tsdata.get_raw_historical_values_asyn(
        start_time=(datetime.now() - timedelta(30)),
        end_time=(datetime.now() - timedelta(29)),
        variable_list=list_of_ids,
    )


@pytest.mark.asyncio
class TestCaseAsyncOPCUA():

    @mock.patch("aiohttp.ClientSession.post")
    async def test_make_request_retries(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        
        error_response = mock.Mock()
        error_response.status = 500
        error_response.raise_for_status.side_effect = ClientResponseError(
            request_info=aiohttp.RequestInfo(
                url=YarlURL("http://example.com"),
                method="POST",
                headers={},
                real_url=YarlURL("http://example.com")
            ),
            history=(),
            status=500
        )
        
        mock_post.side_effect = [error_response, error_response, error_response]
        
        with pytest.raises(RuntimeError):
            await opc._make_request("test_endpoint", {}, 3, 0)
        assert mock_post.call_count == 3

    @mock.patch('aiohttp.ClientSession.post')
    async def test_make_request_client_error(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        
        mock_post.side_effect = aiohttp.ClientError("Test client error")
        
        with pytest.raises(RuntimeError):
            await opc._make_request("test_endpoint", {}, 1, 0)

    @mock.patch("aiohttp.ClientSession.post")
    async def test_make_request_500_error(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)

        error_response = mock.AsyncMock()
        error_response.status = 500

        async def raise_for_status():
            raise ClientResponseError(
                request_info=aiohttp.RequestInfo(
                    url=YarlURL("http://example.com"),
                    method="POST",
                    headers={},
                    real_url=YarlURL("http://example.com")
                ),
                history=(),
                status=500
            )
        error_response.raise_for_status.side_effect = raise_for_status
        mock_post.return_value.__aenter__.return_value = error_response

        with pytest.raises(ClientResponseError):
            await error_response.raise_for_status()  
            await opc._make_request("test_endpoint", {}, 1, 0)

    @mock.patch('aiohttp.ClientSession.post')
    async def test_make_request_max_retries_reached(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        
        error_response = mock.Mock()
        error_response.status = 500
        error_response.raise_for_status.side_effect = ClientResponseError(
            request_info=aiohttp.RequestInfo(
                url=YarlURL("http://example.com"),
                method="POST",
                headers={},
                real_url=YarlURL("http://example.com")
            ),
            history=(),
            status=500
        )
        
        mock_post.side_effect = [error_response, error_response, error_response]
        
        with pytest.raises(RuntimeError):
            await opc._make_request("test_endpoint", {}, 3, 0)
        assert mock_post.call_count == 3

    @mock.patch("aiohttp.ClientSession.post")
    async def test_make_request_successful(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        mock_response = mock.Mock()
        mock_response.status = 200
        mock_response.json = mock.AsyncMock(return_value={"Success": True})
        mock_post.return_value.__aenter__.return_value = mock_response

        result = await opc._make_request("test_endpoint", {}, 3, 0)
        assert result == {"Success": True}

    @mock.patch("aiohttp.ClientSession.post")
    async def test_historical_values_success(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_historical_result, status_code=200
        )
        result = await make_historical_request()
        cols_to_check = ["Value"]
        assert all(
            ptypes.is_numeric_dtype(result[col]) for col in cols_to_check
        )
        assert result["Value"].tolist() == [
            34.28500000000003,
            6.441666666666666,
            34.28500000000003,
            6.441666666666666,
        ]
        assert result["ValueType"].tolist() == [
            "Double",
            "Double",
            "Double",
            "Double",
        ]

    @mock.patch("pyprediktormapclient.opc_ua.OPC_UA._make_request")
    async def test_get_historical_values(self, mock_make_request):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        mock_make_request.return_value = {
            "Success": True,
            "HistoryReadResults": [
                {
                    "NodeId": {"Id": "SOMEID", "Namespace": 1, "IdType": 2},
                    "DataValues": [
                        {"Value": {"Type": 11, "Body": 1.23}, "SourceTimestamp": "2023-01-01T00:00:00Z"}
                    ]
                }
            ]
        }
        
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 2)
        variable_list = ["SOMEID"]
        
        result = await opc.get_historical_values(
            start_time, end_time, variable_list, "test_endpoint",
            lambda vars: [{"NodeId": var} for var in vars]
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["Value.Body"] == 1.23

    @mock.patch("aiohttp.ClientSession.post")
    async def test_get_historical_values_no_results(self, mock_post):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)

        mock_post.return_value = AsyncMockResponse(
            json_data={"Success": True, "HistoryReadResults": []}, status_code=200
        )

        result = await opc.get_historical_values(
            start_time=datetime(2023, 1, 1),
            end_time=datetime(2023, 1, 2),
            variable_list=["SOMEID"],
            endpoint="values/historical",
            prepare_variables=lambda vars: [{"NodeId": var} for var in vars]
        )

        assert result.empty

    @mock.patch("aiohttp.ClientSession.post")
    async def test_get_raw_historical_values_asyn(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_raw_historical_result, status_code=200
        )
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = await opc.get_raw_historical_values_asyn(
            start_time=datetime(2023, 1, 1),
            end_time=datetime(2023, 1, 2),
            variable_list=["SOMEID"],
            limit_start_index=0,
            limit_num_records=100
        )
        assert isinstance(result, pd.DataFrame)
        assert "Value" in result.columns
        assert "Timestamp" in result.columns

    @mock.patch("aiohttp.ClientSession.post")
    async def test_get_historical_aggregated_values_asyn(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_historical_result, status_code=200
        )
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = await opc.get_historical_aggregated_values_asyn(
            start_time=datetime(2023, 1, 1),
            end_time=datetime(2023, 1, 2),
            pro_interval=3600000,
            agg_name="Average",
            variable_list=["SOMEID"]
        )
        assert isinstance(result, pd.DataFrame)
        assert "Value" in result.columns
        assert "Timestamp" in result.columns
        assert "StatusSymbol" in result.columns

    @mock.patch("aiohttp.ClientSession.post")
    async def test_historical_values_no_dict(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_historical_request()

    @mock.patch("aiohttp.ClientSession.post")
    async def test_historical_values_unsuccess(self, mock_post):
        mock_post.return_value = unsuccessful_async_mock_response()
        with pytest.raises(RuntimeError):
            await make_historical_request()

    @mock.patch("aiohttp.ClientSession.post")
    async def test_historical_values_no_hist(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_historical_request()

    @mock.patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_success(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_raw_historical_result, status_code=200
        )
        result = await make_raw_historical_request()
        cols_to_check = ["Value"]
        assert all(
            ptypes.is_numeric_dtype(result[col]) for col in cols_to_check
        )

    @mock.patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_no_dict(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_raw_historical_request()

    @mock.patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_unsuccess(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_raw_historical_request()

    @mock.patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_no_hist(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_raw_historical_request()


if __name__ == "__main__":
    unittest.main()
