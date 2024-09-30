import asyncio
import json
import unittest
from copy import deepcopy
from datetime import date, datetime, timedelta
from typing import List
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pandas as pd
import pandas.api.types as ptypes
import pytest
import requests
from aiohttp.client_exceptions import ClientResponseError
from pydantic import AnyUrl, BaseModel, ValidationError
from pydantic_core import Url
from requests.exceptions import HTTPError
from yarl import URL as YarlURL

from pyprediktormapclient.auth_client import AUTH_CLIENT, Token
from pyprediktormapclient.opc_ua import OPC_UA, TYPE_LIST

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
        self.raise_for_status = Mock(return_value=False)
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


class TestCaseOPCUA:
    @pytest.fixture(autouse=True)
    def setup(self, opc):
        self.opc = opc
        self.auth_client_mock = Mock()
        self.opc_auth = OPC_UA(
            rest_url=URL, opcua_url=OPC_URL, auth_client=self.auth_client_mock
        )
        opc.TYPE_DICT = {t["id"]: t["type"] for t in TYPE_LIST}

    def test_malformed_rest_url(self):
        with pytest.raises(ValidationError):
            AnyUrlModel(rest_url="not_an_url", opcua_url=OPC_URL)

    def test_rest_url_ends_with_slash(self):
        url_with_trailing_slash = self.opc.rest_url.rstrip("/") + "/"
        self.opc.rest_url = url_with_trailing_slash
        assert self.opc.rest_url == URL

    def test_malformed_opcua_url(self):
        with pytest.raises(ValidationError):
            AnyUrlModel(rest_url=URL, opcua_url="not_an_url")

    def test_invalid_opcua_url(self):
        with pytest.raises(ValueError, match="Invalid OPC UA URL"):
            OPC_UA(rest_url=URL, opcua_url="http://invalidurl.com")

    def test_namespaces(self, opc):
        assert "ClientNamespaces" not in opc.body

        opc_with_namespaces = OPC_UA(
            rest_url=URL, opcua_url=OPC_URL, namespaces=["1", "2"]
        )
        assert "ClientNamespaces" in opc_with_namespaces.body
        assert opc_with_namespaces.body["ClientNamespaces"] == ["1", "2"]

    def test_init_with_auth_client(self):
        self.auth_client_mock.token = Mock()
        self.auth_client_mock.token.session_token = "test_token"
        self.opc_auth = OPC_UA(
            rest_url=URL, opcua_url=OPC_URL, auth_client=self.auth_client_mock
        )

        assert self.opc_auth.auth_client == self.auth_client_mock
        assert self.opc_auth.headers["Authorization"] == "Bearer test_token"

    def test_init_with_auth_client_no_token(self):
        self.auth_client_mock.token = None
        opc_auth = OPC_UA(
            rest_url=URL, opcua_url=OPC_URL, auth_client=self.auth_client_mock
        )

        assert "Authorization" not in opc_auth.headers

    def test_init_with_minimal_args(self):
        assert self.opc.auth_client is None
        assert "Authorization" not in self.opc.headers

    def test_json_serial_with_url(self):
        url = Url("http://example.com")
        result = self.opc.json_serial(url)
        assert result == "http://example.com/"
        assert isinstance(result, str)

    def test_json_serial_with_datetime(self):
        dt = datetime(2024, 9, 2, 12, 0, 0)
        result = self.opc.json_serial(dt)
        assert result == "2024-09-02T12:00:00"

    def test_json_serial_with_date(self):
        d = date(2024, 9, 2)
        result = self.opc.json_serial(d)
        assert result == "2024-09-02"

    def test_json_serial_with_unsupported_type(self):
        unsupported_type = set()
        with pytest.raises(TypeError) as exc_info:
            self.opc.json_serial(unsupported_type)

        expected_error_message = (
            f"Type {type(unsupported_type)} not serializable"
        )
        assert str(exc_info.value) == expected_error_message

    def test_check_auth_client_404_error(self):
        self.auth_client_mock.token.session_token = "test_token"
        content = {"error": {"code": 404}}
        self.opc_auth.check_auth_client(content)
        self.auth_client_mock.request_new_ory_token.assert_called_once()
        assert self.opc_auth.headers["Authorization"] == "Bearer test_token"

    def test_check_auth_client_500_error(self):
        self.auth_client_mock.token.session_token = "test_token"
        content = {"error": {"code": 500}, "ErrorMessage": "Server Error"}

        with pytest.raises(RuntimeError) as excinfo:
            self.opc_auth.check_auth_client(content)
        assert str(excinfo.value) == "Server Error"
        self.auth_client_mock.request_new_ory_token.assert_not_called()

    def test_check_auth_client_empty_error(self):
        self.auth_client_mock.token.session_token = "test_token"
        content = {"error": {}}

        with pytest.raises(RuntimeError):
            self.opc_auth.check_auth_client(content)
        self.auth_client_mock.request_new_ory_token.assert_not_called()

    def test_check_auth_client_is_none(self):
        with pytest.raises(Exception):
            self.opc_auth.check_auth_client()

    def test_check_if_ory_session_token_is_valid_refresh_when_expired(self):
        self.auth_client_mock.check_if_token_has_expired.return_value = True
        self.opc_auth.check_if_ory_session_token_is_valid_refresh()
        self.auth_client_mock.check_if_token_has_expired.assert_called_once()
        self.auth_client_mock.refresh_token.assert_called_once()

    def test_check_if_ory_session_token_is_valid_refresh_when_not_expired(
        self,
    ):
        self.auth_client_mock.check_if_token_has_expired.return_value = False
        self.opc_auth.check_if_ory_session_token_is_valid_refresh()
        self.auth_client_mock.check_if_token_has_expired.assert_called_once()
        self.auth_client_mock.refresh_token.assert_not_called()

    def test_get_value_type(self):
        result = self.opc._get_value_type(1)
        assert "id" in result
        assert "type" in result
        assert "description" in result
        assert result["type"] == "Boolean"

        result = self.opc._get_value_type(25)
        assert result["type"] == "DiagnosticInfo"

        result = self.opc._get_value_type(0)
        assert result["id"] == 0
        assert result["type"] == "Null"
        assert result["description"] == "An invalid or unspecified value"

        for invalid_type in [26, 100000]:
            result = self.opc._get_value_type(invalid_type)
            assert result["id"] is None
            assert result["type"] is None
            assert result["description"] is None

    def test_get_variable_list_as_list(self):
        pydantic_var = Variables(Id="SOMEID", Namespace=1, IdType=2)
        result = self.opc._get_variable_list_as_list([pydantic_var])
        assert isinstance(result[0], dict)
        assert result[0] == {"Id": "SOMEID", "Namespace": 1, "IdType": 2}

        dict_var = {"Id": "SOMEID2", "Namespace": 2, "IdType": 1}
        result = self.opc._get_variable_list_as_list([dict_var])
        assert isinstance(result[0], dict)
        assert result[0] == dict_var

        with pytest.raises(
            TypeError, match="Unsupported type in variable_list"
        ):
            self.opc._get_variable_list_as_list([123])

    def test_get_values_variable_list_not_list(self):
        not_a_list = "not_a_list"

        with pytest.raises(
            TypeError, match="Unsupported type in variable_list"
        ):
            self.opc.get_values(not_a_list)

    def test_get_variable_list_as_list_invalid_type(self):
        with pytest.raises(TypeError):
            self.opc._get_variable_list_as_list([1, 2, 3])

    @pytest.mark.parametrize("auth_client_class", [None, AUTH_CLIENT])
    @patch("requests.post", side_effect=successful_mocked_requests)
    def test_get_live_values_successful_response_processing(
        self, mock_get, auth_client_class
    ):
        if auth_client_class:
            auth_client = auth_client_class(
                rest_url=URL, username="test_user", password="test_pass"
            )
            auth_client.token = Token(
                session_token="test_session_id",
                expires_at="2099-01-01T00:00:00Z",
            )
            tsdata = OPC_UA(
                rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client
            )
        else:
            tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)

        result = tsdata.get_values(list_of_ids)

        assert len(result) == len(
            list_of_ids
        ), "Result length should match input length"

        for num, row in enumerate(list_of_ids):
            assert (
                result[num]["Id"] == list_of_ids[num]["Id"]
            ), "IDs should match"
            assert (
                result[num]["Timestamp"]
                == successful_live_response[0]["Values"][num][
                    "ServerTimestamp"
                ]
            ), "Timestamps should match"
            assert (
                result[num]["Value"]
                == successful_live_response[0]["Values"][num]["Value"]["Body"]
            ), "Values should match"

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_get_live_values_with_auth_client_error_handling(
        self, mock_request_from_api
    ):
        self.auth_client_mock.token.session_token = "test_token"

        mock_response = Mock()
        mock_response.content = json.dumps({"error": {"code": 404}}).encode()
        mock_request_from_api.side_effect = [
            requests.exceptions.HTTPError(
                "404 Client Error", response=mock_response
            ),
            successful_live_response,
        ]

        result = self.opc_auth.get_values(list_of_ids)

        assert result is not None
        assert (
            mock_request_from_api.call_count == 2
        ), "request_from_api should be called twice due to retry"

        assert (
            "Authorization" in self.opc_auth.headers
        ), "Authorization header is missing"
        assert "test_token" in self.opc_auth.headers.get(
            "Authorization", ""
        ), "Session token not found in Authorization header"

    @patch("requests.post", side_effect=empty_values_mocked_requests)
    def test_get_live_values_with_missing_values(self, mock_get):
        result = self.opc.get_values(list_of_ids)
        for num, row in enumerate(list_of_ids):
            if num < len(result):
                assert result[num]["Id"] == list_of_ids[num]["Id"]
                assert (
                    result[num]["Timestamp"]
                    == empty_live_response[0]["Values"][num]["ServerTimestamp"]
                )
                assert all(
                    result[num][key] is None for key in ["Value", "ValueType"]
                )

    @patch("requests.post", side_effect=no_status_code_mocked_requests)
    def test_get_live_values_no_status_code(self, mock_get):
        result = self.opc.get_values(list_of_ids)
        assert result[0]["StatusCode"] is None
        assert result[0]["StatusSymbol"] is None
        assert all(
            result[0][key] is not None
            for key in ["Id", "Timestamp", "Value", "ValueType"]
        )

    @patch("requests.post", side_effect=no_mocked_requests)
    def test_get_live_values_no_response(self, mock_get):
        result = self.opc.get_values(list_of_ids)
        for item in result:
            assert all(
                item[key] is None
                for key in [
                    "Timestamp",
                    "Value",
                    "ValueType",
                    "StatusCode",
                    "StatusSymbol",
                ]
            )
        assert "Success" not in result[0]

    @patch("requests.post", side_effect=empty_mocked_requests)
    def test_get_live_values_empty_response(self, mock_get):
        result = self.opc.get_values(list_of_ids)

        assert len(result) == len(
            list_of_ids
        ), "Result should have same length as input"

        for item in result:
            assert all(
                item[key] is None
                for key in [
                    "Timestamp",
                    "Value",
                    "ValueType",
                    "StatusCode",
                    "StatusSymbol",
                ]
            )
            assert all(
                item[key] is not None for key in ["Id", "Namespace", "IdType"]
            )

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_get_live_values_error_handling(self, mock_request):
        mock_request.side_effect = Exception("Test exception")
        with pytest.raises(RuntimeError) as exc_info:
            self.opc.get_values(list_of_ids)
        assert str(exc_info.value) == "Error in get_values: Test exception"

    @patch("requests.post")
    def test_get_live_values_500_error(self, mock_post):
        error_response = Mock()
        error_response.status_code = 500
        error_response.raise_for_status.side_effect = HTTPError(
            "500 Server Error: Internal Server Error for url"
        )

        mock_post.return_value = error_response
        with pytest.raises(RuntimeError):
            self.opc.get_values(list_of_ids)

    @patch("requests.post", side_effect=unsuccessful_mocked_requests)
    def test_get_live_values_unsuccessful(self, mock_post):
        with pytest.raises(RuntimeError):
            self.opc.get_values(list_of_ids)

    def test_check_content_valid(self):
        valid_content = successful_historical_result.copy()
        self.opc._check_content(valid_content)

    def test_check_content_not_dict(self):
        with pytest.raises(
            RuntimeError, match="No content returned from the server"
        ):
            self.opc._check_content("Not a dictionary")

    def test_check_content_not_successful(self):
        unsuccessful_content = successful_historical_result.copy()
        unsuccessful_content["Success"] = False
        unsuccessful_content["ErrorMessage"] = "Some error occurred"

        with pytest.raises(RuntimeError, match="Some error occurred"):
            self.opc._check_content(unsuccessful_content)

    def test_check_content_no_history_results(self):
        content_without_history = successful_historical_result.copy()
        del content_without_history["HistoryReadResults"]

        with pytest.raises(
            RuntimeError,
            match="No history read results returned from the server",
        ):
            self.opc._check_content(content_without_history)

    def test_process_df_with_value_type(self):
        df_input = pd.DataFrame(
            {
                "Value.Type": [11, 12, 11],
                "Value.Body": [1.23, "test", 4.56],
                "OldColumn1": ["A", "B", "C"],
                "OldColumn2": [1, 2, 3],
            }
        )
        columns = {"OldColumn1": "NewColumn1", "OldColumn2": "NewColumn2"}

        result = self.opc._process_df(df_input, columns)

        assert list(result["Value.Type"]) == ["Double", "String", "Double"]
        assert set(result.columns) == {
            "Value.Type",
            "Value.Body",
            "NewColumn1",
            "NewColumn2",
        }

    def test_process_df_without_value_type(self):
        df_input = pd.DataFrame(
            {
                "Value.Body": [1.23, "test", 4.56],
                "OldColumn1": ["A", "B", "C"],
                "OldColumn2": [1, 2, 3],
            }
        )
        columns = {"OldColumn1": "NewColumn1", "OldColumn2": "NewColumn2"}

        result = self.opc._process_df(df_input, columns)

        assert set(result.columns) == {
            "Value.Body",
            "NewColumn1",
            "NewColumn2",
        }

    def test_process_df_rename_error(self):
        df_input = pd.DataFrame(
            {
                "Value.Body": [1.23, "test", 4.56],
                "OldColumn1": ["A", "B", "C"],
            }
        )
        columns = {"NonExistentColumn": "NewColumn"}

        with pytest.raises(KeyError):
            self.opc._process_df(df_input, columns)

    def test_process_df_empty_dataframe(self):
        df_input = pd.DataFrame()
        columns = {}

        result = self.opc._process_df(df_input, columns)

        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_process_content(self):
        result = self.opc._process_content(successful_historical_result)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4

        assert "HistoryReadResults.NodeId.Id" in result.columns
        assert "HistoryReadResults.NodeId.Namespace" in result.columns
        assert "HistoryReadResults.NodeId.IdType" in result.columns

        assert result.iloc[0]["HistoryReadResults.NodeId.Id"] == "SOMEID"
        assert result.iloc[0]["Value.Body"] == 34.28500000000003
        assert result.iloc[0]["SourceTimestamp"] == "2022-09-13T13:39:51Z"

        assert result.iloc[2]["HistoryReadResults.NodeId.Id"] == "SOMEID2"
        assert result.iloc[2]["Value.Body"] == 34.28500000000003
        assert result.iloc[2]["SourceTimestamp"] == "2022-09-13T13:39:51Z"

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_write_live_values_http_error_handling(self, mock_request):
        auth_client_mock = Mock()
        opc = OPC_UA(
            rest_url=URL, opcua_url=OPC_URL, auth_client=auth_client_mock
        )

        mock_response = Mock()
        mock_response.content = json.dumps({"error": {"code": 404}}).encode()
        http_error = requests.exceptions.HTTPError(
            "404 Client Error", response=mock_response
        )
        mock_request.side_effect = [
            http_error,
            {"Success": True, "StatusCodes": [{"Code": 0}]},
        ]

        opc.check_auth_client = Mock()

        result = opc.write_values(list_of_write_values)

        opc.check_auth_client.assert_called_once_with({"error": {"code": 404}})
        assert result is not None
        assert len(result) == len(list_of_write_values)

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_write_live_values_error_handling(self, mock_request):
        mock_request.side_effect = Exception("Test exception")
        with pytest.raises(RuntimeError) as exc_info:
            self.opc.write_values(list_of_write_values)
        assert str(exc_info.value) == "Error in write_values: Test exception"

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_write_live_values_http_error_without_auth(self, mock_request):
        mock_request.side_effect = requests.exceptions.HTTPError(
            "404 Client Error"
        )

        with pytest.raises(RuntimeError) as exc_info:
            self.opc.write_values(list_of_write_values)
        assert str(exc_info.value).startswith(
            "Error in write_values: 404 Client Error"
        )

    @patch("requests.post", side_effect=successful_write_mocked_requests)
    def test_write_live_values_successful(self, mock_get):
        result = self.opc.write_values(list_of_write_values)
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

    @patch("requests.post", side_effect=empty_write_values_mocked_requests)
    def test_write_live_values_with_missing_value_and_statuscode(
        self, mock_get
    ):
        result = self.opc.write_values(list_of_write_values)
        for num, row in enumerate(list_of_write_values):
            assert result[num]["WriteSuccess"] is False

    @patch("requests.post", side_effect=no_write_mocked_requests)
    def test_get_write_live_values_no_response(self, mock_get):
        result = self.opc.write_values(list_of_write_values)
        assert result is None

    @patch("requests.post", side_effect=unsuccessful_write_mocked_requests)
    def test_get_write_live_values_unsuccessful(self, mock_get):
        with pytest.raises(RuntimeError):
            self.opc.write_values(list_of_write_values)

    @patch("requests.post", side_effect=empty_write_mocked_requests)
    def test_get_write_live_values_empty(self, mock_get):
        with pytest.raises(ValueError):
            self.opc.write_values(list_of_write_values)

    def test_write_historical_values_succeeds_with_empty_update_values(self):
        empty_historical_variable = WriteHistoricalVariables(
            NodeId=Variables(**list_of_write_values[0]["NodeId"]),
            PerformInsertReplace=1,
            UpdateValues=[],
        )

        with patch(
            "pyprediktormapclient.opc_ua.request_from_api"
        ) as mock_request:
            mock_request.return_value = {
                "Success": True,
                "HistoryUpdateResults": [{}],
            }
            result = self.opc.write_historical_values(
                [empty_historical_variable.model_dump()]
            )

        assert result is not None
        assert len(result) == 1
        assert result[0].get("WriteSuccess", False)

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_write_historical_values_http_error_handling(self, mock_request):
        mock_response = Mock()
        mock_response.content = json.dumps({"error": {"code": 404}}).encode()
        http_error = requests.exceptions.HTTPError(
            "404 Client Error", response=mock_response
        )
        mock_request.side_effect = [
            http_error,
            {"Success": True, "HistoryUpdateResults": [{}]},
        ]

        self.opc_auth.check_auth_client = Mock()

        converted_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_write_historical_values
        ]
        result = self.opc_auth.write_historical_values(converted_data)

        self.opc_auth.check_auth_client.assert_called_once_with(
            {"error": {"code": 404}}
        )
        assert result is not None
        assert len(result) == len(converted_data)

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_error_handling_write_historical_values(self, mock_request):
        mock_request.side_effect = [
            Exception("Test exception"),
            Exception("Test exception"),
        ]
        input_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_write_historical_values
        ]
        with pytest.raises(Exception) as exc_info:
            self.opc.write_historical_values(input_data)
        assert (
            str(exc_info.value)
            == "Error in write_historical_values: Test exception"
        )
        assert mock_request.call_count == 1

    @patch("pyprediktormapclient.opc_ua.request_from_api")
    def test_write_historical_values_http_error_without_auth(
        self, mock_request
    ):
        mock_request.side_effect = requests.exceptions.HTTPError(
            "404 Client Error"
        )
        with pytest.raises(RuntimeError) as exc_info:
            self.opc.write_historical_values(list_of_write_historical_values)
        assert (
            str(exc_info.value)
            == "Error in write_historical_values: 404 Client Error"
        )

    @patch(
        "requests.post",
        side_effect=successful_write_historical_mocked_requests,
    )
    def test_write_historical_values_successful(self, mock_get):
        converted_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_write_historical_values
        ]
        result = self.opc.write_historical_values(converted_data)
        assert all(row.get("WriteSuccess", False) for row in result)

    @patch(
        "requests.post",
        side_effect=successful_write_historical_mocked_requests,
    )
    def test_write_wrong_order_historical_values_successful(self, mock_get):
        converted_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_write_historical_values_in_wrong_order
        ]
        with pytest.raises(ValueError):
            self.opc.write_historical_values(converted_data)

    @patch("requests.post", side_effect=empty_write_historical_mocked_requests)
    def test_write_historical_values_with_missing_value_and_statuscode(
        self, mock_get
    ):
        converted_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_write_historical_values
        ]
        with pytest.raises(ValueError):
            self.opc.write_historical_values(converted_data)

    @patch("requests.post", side_effect=no_write_mocked_historical_requests)
    def test_get_write_historical_values_no_response(self, mock_get):
        converted_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_write_historical_values
        ]
        result = self.opc.write_historical_values(converted_data)
        assert result is None

    @patch(
        "requests.post",
        side_effect=unsuccessful_write_historical_mocked_requests,
    )
    def test_get_write_historical_values_unsuccessful(self, mock_get):
        converted_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_write_historical_values
        ]
        with pytest.raises(RuntimeError):
            self.opc.write_historical_values(converted_data)

    @patch(
        "requests.post",
        side_effect=successful_write_historical_with_errors_mocked_requests,
    )
    def test_get_write_historical_values_successful_with_error_codes(
        self, mock_get
    ):
        converted_data = [
            WriteHistoricalVariables(**item).model_dump()
            for item in list_of_historical_values_wrong_type_and_value
        ]
        result = self.opc.write_historical_values(converted_data)
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


async def make_historical_request(opc):
    return await opc.get_historical_aggregated_values_asyn(
        start_time=(datetime.now() - timedelta(30)),
        end_time=(datetime.now() - timedelta(29)),
        pro_interval=3600000,
        agg_name="Average",
        variable_list=list_of_ids,
    )


async def make_raw_historical_request(opc):
    return await opc.get_historical_raw_values_asyn(
        start_time=(datetime.now() - timedelta(30)),
        end_time=(datetime.now() - timedelta(29)),
        variable_list=list_of_ids,
    )


@pytest.mark.asyncio
class TestCaseAsyncOPCUA:
    @pytest.fixture(autouse=True)
    def setup(self, opc):
        self.opc = opc
        yield

    async def test_run_coroutine(self):
        async def sample_coroutine():
            await asyncio.sleep(0.1)
            return "Hello, World!"

        result = self.opc.helper.run_coroutine(sample_coroutine())
        assert result == "Hello, World!"

    async def test_run_coroutine_with_exception(self):
        async def failing_coroutine():
            await asyncio.sleep(0.1)
            raise ValueError("Test exception")

        with pytest.raises(ValueError, match="Test exception"):
            self.opc.helper.run_coroutine(failing_coroutine())

    @patch("aiohttp.ClientSession.post")
    async def test_make_request_max_retries(self, mock_post):
        error_response = AsyncMock()
        error_response.status = 500
        error_response.raise_for_status.side_effect = ClientResponseError(
            request_info=aiohttp.RequestInfo(
                url=YarlURL("http://example.com"),
                method="POST",
                headers={},
                real_url=YarlURL("http://example.com"),
            ),
            history=(),
            status=500,
        )

        mock_post.side_effect = [error_response] * 3

        with pytest.raises(RuntimeError) as exc_info:
            await self.opc._make_request("test_endpoint", {}, 3, 0)

        assert mock_post.call_count == 3
        assert "Max retries reached" in str(exc_info.value)

    @patch("aiohttp.ClientSession.post")
    @patch("asyncio.sleep", return_value=None)
    async def test_make_request_all_retries_fail(self, mock_sleep, mock_post):
        error_response = AsyncMock()
        error_response.status = 400
        error_response.text.return_value = "Bad Request"
        error_response.raise_for_status.side_effect = ClientResponseError(
            request_info=aiohttp.RequestInfo(
                url=YarlURL("http://example.com"),
                method="POST",
                headers={},
                real_url=YarlURL("http://example.com"),
            ),
            history=(),
            status=400,
        )
        mock_post.return_value.__aenter__.return_value = error_response
        max_retries = 3
        retry_delay = 0

        with pytest.raises(RuntimeError, match="Max retries reached"):
            await self.opc._make_request(
                "test_endpoint", {}, max_retries, retry_delay
            )

        assert mock_post.call_count == max_retries
        assert mock_sleep.call_count == max_retries - 1

    @patch("aiohttp.ClientSession.post")
    async def test_make_request_client_error(self, mock_post):
        mock_post.side_effect = aiohttp.ClientError("Test client error")

        with pytest.raises(RuntimeError):
            await self.opc._make_request("test_endpoint", {}, 1, 0)

    @patch("aiohttp.ClientSession.post")
    @patch("asyncio.sleep", return_value=None)
    async def test_make_request_non_500_error_with_retry(
        self, mock_sleep, mock_post, mock_error_response
    ):
        mock_error_response.status = 400
        mock_error_response.text.return_value = "Bad Request"
        mock_error_response.raise_for_status.side_effect.status = 400
        mock_error_response.raise_for_status.side_effect.message = (
            "Bad Request"
        )

        success_response = AsyncMock()
        success_response.status = 200
        success_response.json.return_value = {"success": True}

        mock_post.side_effect = [
            AsyncMock(__aenter__=AsyncMock(return_value=mock_error_response)),
            AsyncMock(__aenter__=AsyncMock(return_value=mock_error_response)),
            AsyncMock(__aenter__=AsyncMock(return_value=success_response)),
        ]

        result = await self.opc._make_request("test_endpoint", {}, 3, 0)

        assert result == {"success": True}
        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2

        mock_error_response.text.assert_awaited()
        mock_error_response.raise_for_status.assert_called()

        success_response.json.assert_awaited()

    @patch("aiohttp.ClientSession.post")
    async def test_make_request_successful(self, mock_post):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = {"Success": True}
        mock_post.return_value.__aenter__.return_value = mock_response

        result = await self.opc._make_request("test_endpoint", {}, 3, 0)
        assert result == {"Success": True}
        assert mock_post.call_count == 1

    @patch("aiohttp.ClientSession.post")
    async def test_historical_values_success(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_historical_result, status_code=200
        )
        result = await make_historical_request(opc=self.opc)
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

    @patch("pyprediktormapclient.opc_ua.OPC_UA._make_request")
    async def test_get_historical_values(self, mock_make_request):
        mock_make_request.return_value = {
            "Success": True,
            "HistoryReadResults": [
                {
                    "NodeId": {"Id": "SOMEID", "Namespace": 1, "IdType": 2},
                    "DataValues": [
                        {
                            "Value": {"Type": 11, "Body": 1.23},
                            "SourceTimestamp": "2023-01-01T00:00:00Z",
                        }
                    ],
                }
            ],
        }

        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 2)
        variable_list = ["SOMEID"]

        result = await self.opc.get_historical_values(
            start_time,
            end_time,
            variable_list,
            "test_endpoint",
            lambda vars: [{"NodeId": var} for var in vars],
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["Value.Body"] == 1.23

    @patch("aiohttp.ClientSession.post")
    async def test_get_historical_values_no_results(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data={"Success": True, "HistoryReadResults": []},
            status_code=200,
        )

        result = await self.opc.get_historical_values(
            start_time=datetime(2023, 1, 1),
            end_time=datetime(2023, 1, 2),
            variable_list=["SOMEID"],
            endpoint="values/historical",
            prepare_variables=lambda vars: [{"NodeId": var} for var in vars],
        )

        assert result.empty

    @patch("aiohttp.ClientSession.post")
    async def test_get_historical_raw_values_asyn(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_raw_historical_result, status_code=200
        )
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = await opc.get_historical_raw_values_asyn(
            start_time=datetime(2023, 1, 1),
            end_time=datetime(2023, 1, 2),
            variable_list=["SOMEID"],
            limit_start_index=0,
            limit_num_records=100,
        )
        assert isinstance(result, pd.DataFrame)
        assert "Value" in result.columns
        assert "Timestamp" in result.columns

    async def test_get_raw_historical_values_success(self):
        mock_result = AsyncMock()

        with patch.object(
            self.opc,
            "get_historical_raw_values_asyn",
            return_value=mock_result,
        ):
            with patch.object(
                self.opc.helper, "run_coroutine", return_value=mock_result
            ) as mock_run_coroutine:
                result = self.opc.get_historical_raw_values(
                    start_time=(datetime.now() - timedelta(30)),
                    end_time=(datetime.now() - timedelta(29)),
                    variable_list=list_of_ids,
                )

                mock_run_coroutine.assert_called_once()
                assert result == mock_result

    async def test_get_raw_historical_values_with_args(self):
        mock_result = AsyncMock()

        with patch.object(
            self.opc,
            "get_historical_raw_values_asyn",
            return_value=mock_result,
        ) as mock_async:
            result = await make_raw_historical_request(self.opc)

            mock_async.assert_called_once()
            args, kwargs = mock_async.call_args
            assert "start_time" in kwargs
            assert "end_time" in kwargs
            assert kwargs["variable_list"] == list_of_ids
            assert result == mock_result

    async def test_get_raw_historical_values_exception(self):
        with patch.object(
            self.opc,
            "get_historical_raw_values_asyn",
            side_effect=Exception("Test exception"),
        ):
            with pytest.raises(Exception, match="Test exception"):
                await make_raw_historical_request(self.opc)

    @patch("aiohttp.ClientSession.post")
    async def test_get_historical_aggregated_values_asyn(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_historical_result, status_code=200
        )
        result = await self.opc.get_historical_aggregated_values_asyn(
            start_time=datetime(2023, 1, 1),
            end_time=datetime(2023, 1, 2),
            pro_interval=3600000,
            agg_name="Average",
            variable_list=["SOMEID"],
        )
        assert isinstance(result, pd.DataFrame)
        assert "Value" in result.columns
        assert "Timestamp" in result.columns
        assert "StatusSymbol" in result.columns

    async def test_get_historical_aggregated_values_success(self):
        mock_result = AsyncMock()

        with patch.object(
            self.opc,
            "get_historical_aggregated_values_asyn",
            return_value=mock_result,
        ):
            with patch.object(
                self.opc.helper, "run_coroutine", return_value=mock_result
            ) as mock_run_coroutine:
                result = self.opc.get_historical_aggregated_values(
                    start_time=(datetime.now() - timedelta(30)),
                    end_time=(datetime.now() - timedelta(29)),
                    pro_interval=3600000,
                    agg_name="Average",
                    variable_list=list_of_ids,
                )

                mock_run_coroutine.assert_called_once()
                assert result == mock_result

    async def test_get_historical_aggregated_values_with_args(self):
        mock_result = AsyncMock()

        with patch.object(
            self.opc,
            "get_historical_aggregated_values_asyn",
            return_value=mock_result,
        ) as mock_async:
            result = await self.opc.get_historical_aggregated_values_asyn(
                start_time=(datetime.now() - timedelta(30)),
                end_time=(datetime.now() - timedelta(29)),
                pro_interval=3600000,
                agg_name="Average",
                variable_list=list_of_ids,
            )

            mock_async.assert_called_once()
            args, kwargs = mock_async.call_args
            assert "start_time" in kwargs
            assert "end_time" in kwargs
            assert kwargs["pro_interval"] == 3600000
            assert kwargs["agg_name"] == "Average"
            assert kwargs["variable_list"] == list_of_ids
            assert result == mock_result

    async def test_get_historical_aggregated_values_exception(self):
        with patch.object(
            self.opc,
            "get_historical_aggregated_values_asyn",
            side_effect=Exception("Test exception"),
        ):
            with patch.object(
                self.opc.helper,
                "run_coroutine",
                side_effect=Exception("Test exception"),
            ):
                with pytest.raises(Exception, match="Test exception"):
                    self.opc.get_historical_aggregated_values(
                        start_time=(datetime.now() - timedelta(30)),
                        end_time=(datetime.now() - timedelta(29)),
                        pro_interval=3600000,
                        agg_name="Average",
                        variable_list=list_of_ids,
                    )

    @patch("aiohttp.ClientSession.post")
    async def test_historical_values_no_dict(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_historical_request(opc=self.opc)

    @patch("aiohttp.ClientSession.post")
    async def test_historical_values_unsuccess(self, mock_post):
        mock_post.return_value = unsuccessful_async_mock_response()
        with pytest.raises(RuntimeError):
            await make_historical_request(opc=self.opc)

    @patch("aiohttp.ClientSession.post")
    async def test_historical_values_no_hist(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_historical_request(opc=self.opc)

    @patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_success(self, mock_post):
        mock_post.return_value = AsyncMockResponse(
            json_data=successful_raw_historical_result, status_code=200
        )
        result = await make_raw_historical_request(opc=self.opc)
        cols_to_check = ["Value"]
        assert all(
            ptypes.is_numeric_dtype(result[col]) for col in cols_to_check
        )

    @patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_no_dict(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_raw_historical_request(opc=self.opc)

    @patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_unsuccess(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_raw_historical_request(opc=self.opc)

    @patch("aiohttp.ClientSession.post")
    async def test_raw_historical_values_no_hist(self, mock_post):
        with pytest.raises(RuntimeError):
            await make_raw_historical_request(opc=self.opc)


if __name__ == "__main__":
    unittest.main()
