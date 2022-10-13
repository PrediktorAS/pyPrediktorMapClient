import requests
import unittest
from unittest import mock
import pytest
from pydantic import ValidationError
from copy import deepcopy
import datetime
import pandas as pd
import pandas.api.types as ptypes

from pyprediktormapclient.opc_ua import OPC_UA, Variables

URL = "http://someserver.somedomain.com/v1/"
OPC_URL = "opc.tcp://nosuchserver.nosuchdomain.com"

list_of_ids = [
    {"Id": "SOMEID", "Namespace": 1, "IdType": 2},
    {"Id": "SOMEID2", "Namespace": 1, "IdType": 2},
]

successful_live_response = [
    {
        "Success": True,
        "ErrorMessage": "",
        "ErrorCode": 0,
        "ServerNamespaces": ["string"],
        "Values": [
            {
                "Value": {"Type": 1, "Body": True},
                "StatusCode": {"Code": 0, "Symbol": "Good"},
                "SourceTimestamp": "2022-09-21T13:13:38.183Z",
                "ServerTimestamp": "2022-09-21T13:13:38.183Z",
            },
            {
                "Value": {"Type": 2, "Body": False},
                "StatusCode": {"Code": 1, "Symbol": "Good"},
                "SourceTimestamp": "2023-09-21T13:13:38.183Z",
                "ServerTimestamp": "2023-09-21T13:13:38.183Z",
            },
        ],
    }
]

empty_live_response = [
    {
        "Success": True,
        "ErrorMessage": "",
        "ErrorCode": 0,
        "ServerNamespaces": ["string"],
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
        }
    ]
}

class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code
        self.raise_for_status = mock.Mock(return_value=False)

    def json(self):
        return self.json_data


# This method will be used by the mock to replace requests
def successful_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/get":
        return MockResponse(successful_live_response, 200)

    return MockResponse(None, 404)


def empty_values_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}values/get":
        return MockResponse(empty_live_response, 200)

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

def successful_mocked_historical_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalaggregated":
        return MockResponse(successful_historical_result, 200)

    return MockResponse(None, 404)

def no_dict_mocked_historical_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalaggregated":
        return MockResponse([], 200)

    return MockResponse(None, 404)

def unsuccessful_mocked_historical_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalaggregated":
        unsuc = deepcopy(successful_historical_result)
        unsuc.pop("HistoryReadResults")
        return MockResponse([unsuc], 200)

    return MockResponse(None, 404)

def no_hosticalresult_mocked_historical_requests(*args, **kwargs):
    if args[0] == f"{URL}values/historicalaggregated":
        unsuc = deepcopy(successful_historical_result)
        unsuc["Success"] = False
        return MockResponse([unsuc], 200)

    return MockResponse(None, 404)

def make_historical_request():
    tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
    return tsdata.get_historical_aggregated_values(
                start_time=(datetime.datetime.now() - datetime.timedelta(30)),
                end_time=(datetime.datetime.now() - datetime.timedelta(29)),
                pro_interval=3600000,
                agg_name="Average",
                variable_list=list_of_ids,
            )

# Our test case class
class OPCUATestCase(unittest.TestCase):
    def test_malformed_rest_url(self):
        with pytest.raises(ValidationError):
            OPC_UA(rest_url="not_an_url", opcua_url=OPC_URL)

    def test_malformed_opcua_url(self):
        with pytest.raises(ValidationError):
            OPC_UA(rest_url=URL, opcua_url="not_an_url")

    def test_namespaces(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL, namespaces=["1", "2"])
        assert "ClientNamespaces" in opc.body
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        assert "ClientNamespaces" not in opc.body

    def test_get_value_type(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result_none = opc._get_value_type(100000)
        assert result_none["id"] == None
        result = opc._get_value_type(1)
        assert "id" in result
        assert "type" in result
        assert "description" in result
        assert result["type"] == "Boolean"

    def test_get_variable_list_as_list(self):
        opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        var = Variables(Id="ID", Namespace=1, IdType=2)
        list = [var]
        result = opc._get_variable_list_as_list(list)
        assert "Id" in result[0]
        assert result[0]["Id"] == "ID"

    @mock.patch("requests.post", side_effect=successful_mocked_requests)
    def test_get_live_values_successful(self, mock_get):
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
            assert (
                result[num]["StatusCode"]
                == successful_live_response[0]["Values"][num]["StatusCode"]["Code"]
            )
            assert (
                result[num]["StatusSymbol"]
                == successful_live_response[0]["Values"][num]["StatusCode"]["Symbol"]
            )

    @mock.patch("requests.post", side_effect=empty_values_mocked_requests)
    def test_get_live_values_with_missing_value_and_statuscode(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.get_values(list_of_ids)
        for num, row in enumerate(list_of_ids):
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
        assert result[0]["Timestamp"] == None

    @mock.patch("requests.post", side_effect=unsuccessful_mocked_requests)
    def test_get_live_values_unsuccessful(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        with pytest.raises(RuntimeError):
            result = tsdata.get_values(list_of_ids)

    @mock.patch("requests.post", side_effect=empty_mocked_requests)
    def test_get_live_values_empty(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.get_values(list_of_ids)
        assert result[0]["Timestamp"] == None

    @mock.patch("requests.post", side_effect=no_status_code_mocked_requests)
    def test_get_live_values_no_status_code(self, mock_get):
        tsdata = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
        result = tsdata.get_values(list_of_ids)
        assert result[0]["StatusCode"] == None

    @mock.patch("requests.post", side_effect=successful_mocked_historical_requests)
    def test_historical_values_success(self, mock_get):
        result = make_historical_request()
        cols_to_check = ["Value"]
        assert all(ptypes.is_numeric_dtype(result[col]) for col in cols_to_check)
        assert result['Value'].tolist() == [34.28500000000003, 6.441666666666666, 34.28500000000003, 6.441666666666666]
        assert result['ValueType'].tolist() == ["Double", "Double", "Double", "Double"]

    @mock.patch("requests.post", side_effect=no_dict_mocked_historical_requests)
    def test_historical_values_no_dict(self, mock_get):
        with pytest.raises(RuntimeError):
            make_historical_request()

    @mock.patch("requests.post", side_effect=unsuccessful_mocked_historical_requests)
    def test_historical_values_unsuccess(self, mock_get):
        with pytest.raises(RuntimeError):
            make_historical_request()

    @mock.patch("requests.post", side_effect=no_hosticalresult_mocked_historical_requests)
    def test_historical_values_no_hist(self, mock_get):
        with pytest.raises(RuntimeError):
            make_historical_request()
        
if __name__ == "__main__":
    unittest.main()
