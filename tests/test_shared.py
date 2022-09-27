import unittest
from unittest import mock
import requests
import pytest
from pydantic import ValidationError

from pyprediktormapclient.shared import request_from_api

URL = "http://someserver.somedomain.com/v1/"
return_json = [
    {
        "Id": "6:0:1029",
        "DisplayName": "IPVBaseCalculate",
        "BrowseName": "IPVBaseCalculate",
        "Props": [],
        "Vars": [],
    }
]

# This method will be used by the mock to replace requests
def mocked_requests(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.raise_for_status = mock.Mock(return_value=False)

        def json(self):
            return self.json_data

    if args[0] == f"{URL}something":
        return MockResponse(return_json, 200)

    return MockResponse(None, 404)


class AnalyticsHelperTestCase(unittest.TestCase):
    def test_requests_with_malformed_url(self):
        with pytest.raises(ValidationError):
            request_from_api(rest_url="No_valid_url", method="GET", endpoint="/")

    def test_requests_with_unsupported_method(self):
        with pytest.raises(ValidationError):
            request_from_api(rest_url=URL, method="NO_SUCH_METHOD", endpoint="/")

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_request_from_api_method_get(self, mock_get):
        result = request_from_api(rest_url=URL, method="GET", endpoint="something")
        assert result == return_json

    @mock.patch("requests.post", side_effect=mocked_requests)
    def test_request_from_api_method_post(self, mock_get):
        result = request_from_api(
            rest_url=URL, method="POST", endpoint="something", data="test"
        )
        assert result == return_json


if __name__ == "__main__":
    unittest.main()
