import unittest
from unittest import mock

import pytest
import requests
from requests.exceptions import RequestException

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


class MockResponse:
    def __init__(self, json_data, status_code, headers=None):
        self.json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}
        self.text = str(json_data)

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RequestException(f"HTTP Error: {self.status_code}")


def mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}something":
        return MockResponse(
            return_json, 200, {"Content-Type": "application/json"}
        )
    return MockResponse(None, 404)


class AnalyticsHelperTestCase(unittest.TestCase):
    def test_requests_with_malformed_url(self):
        with pytest.raises(requests.exceptions.MissingSchema):
            request_from_api(
                rest_url="No_valid_url", method="GET", endpoint="/"
            )

    def test_requests_with_unsupported_method(self):
        with pytest.raises(TypeError):
            request_from_api(
                rest_url=URL, method="NO_SUCH_METHOD", endpoint="/"
            )

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_request_from_api_method_get(self, mock_get):
        result = request_from_api(
            rest_url=URL, method="GET", endpoint="something"
        )
        assert result == return_json

    @mock.patch("requests.post", side_effect=mocked_requests)
    def test_request_from_api_method_post(self, mock_get):
        result = request_from_api(
            rest_url=URL, method="POST", endpoint="something", data="test"
        )
        assert result == return_json

    @mock.patch("requests.get")
    def test_request_from_api_json_response(self, mock_get):
        json_response = {"key": "value"}
        mock_get.return_value = MockResponse(
            json_response, 200, headers={"Content-Type": "application/json"}
        )

        result = request_from_api(rest_url=URL, method="GET", endpoint="test")

        self.assertEqual(result, json_response)

    @mock.patch("requests.get")
    def test_request_from_api_non_json_response(self, mock_get):
        non_json_response = "This is a plain text response"
        mock_get.return_value = MockResponse(
            non_json_response, 200, headers={"Content-Type": "text/plain"}
        )

        result = request_from_api(rest_url=URL, method="GET", endpoint="test")

        self.assertEqual(
            result,
            {"error": "Non-JSON response", "content": non_json_response},
        )
        mock_get.assert_called_once_with(
            f"{URL}test", timeout=(3, 27), params=None, headers=None
        )

    @mock.patch("requests.get")
    def test_request_from_api_exception(self, mock_get):
        mock_get.side_effect = RequestException("Network error")
        with self.assertRaises(RequestException):
            request_from_api(rest_url=URL, method="GET", endpoint="test")

    @mock.patch("requests.get")
    def test_request_from_api_extended_timeout(self, mock_get):
        mock_get.return_value = MockResponse(
            return_json, 200, {"Content-Type": "application/json"}
        )
        result = request_from_api(
            rest_url=URL,
            method="GET",
            endpoint="something",
            extended_timeout=True,
        )
        mock_get.assert_called_with(
            f"{URL}something", timeout=(3, 300), params=None, headers=None
        )
        assert result == return_json

    @mock.patch("requests.Session")
    def test_request_from_api_with_session(self, mock_session):
        mock_session_instance = mock_session.return_value
        mock_session_instance.get.return_value = MockResponse(
            return_json, 200, {"Content-Type": "application/json"}
        )

        session = requests.Session()
        result = request_from_api(
            rest_url=URL, method="GET", endpoint="something", session=session
        )

        mock_session_instance.get.assert_called_with(
            f"{URL}something", timeout=(3, 27), params=None, headers=None
        )
        assert result == return_json

    @mock.patch("requests.post")
    def test_request_from_api_with_params_and_headers(self, mock_post):
        mock_post.return_value = MockResponse(
            return_json, 200, {"Content-Type": "application/json"}
        )
        params = {"param": "value"}
        headers = {"Authorization": "Bearer token"}

        result = request_from_api(
            rest_url=URL,
            method="POST",
            endpoint="something",
            data="test_data",
            params=params,
            headers=headers,
        )
        mock_post.assert_called_with(
            f"{URL}something",
            data="test_data",
            headers=headers,
            timeout=(3, 27),
            params=params,
        )
        assert result == return_json

    @mock.patch("requests.get")
    def test_request_from_api_http_error(self, mock_get):
        mock_get.return_value = MockResponse(None, 404)
        with pytest.raises(RequestException):
            request_from_api(rest_url=URL, method="GET", endpoint="something")


if __name__ == "__main__":
    unittest.main()
