import unittest
from datetime import date, datetime
from unittest.mock import Mock, patch
from urllib.parse import urlparse

import pytest
import requests
from pydantic import AnyUrl, BaseModel, ValidationError
from pydantic_core import Url

from pyprediktormapclient.model_index import ModelIndex

URL = "http://someserver.somedomain.com/v1/"
object_types = [
    {
        "Id": "6:0:1029",
        "DisplayName": "IPVBaseCalculate",
        "BrowseName": "IPVBaseCalculate",
        "Props": [],
        "Vars": [],
    }
]

namespaces = [{"Idx": 0, "Uri": "http://opcfoundation.org/UA/"}]

objects_of_type = [
    {
        "Id": "3:1:SSO.EG-AS",
        "Type": "6:0:1009",
        "Subtype": "6:0:1009",
        "DisplayName": "EG-AS",
        "Props": [{"DisplayName": "GPSLatitude", "Value": "24.44018"}],
        "Vars": [
            {
                "DisplayName": "Alarm.CommLossPlantDevice",
                "Id": "3:1:SSO.EG-AS.Signals.Alarm.CommLossPlantDevice",
            }
        ],
    }
]

descendants = [
    {
        "ObjectId": "string",
        "DescendantId": "string",
        "DescendantName": "string",
        "DescendantType": "string",
        "ObjectName": "string",
        "Props": [{"DisplayName": "string", "Value": "string"}],
        "Vars": [{"DisplayName": "string", "Id": "string"}],
    }
]

ancestors = [
    {
        "ObjectId": "string",
        "AncestorId": "string",
        "AncestorName": "string",
        "AncestorType": "string",
        "ObjectName": "string",
        "Props": [{"DisplayName": "string", "Value": "string"}],
        "Vars": [{"DisplayName": "string", "Id": "string"}],
    }
]


def mocked_requests(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.headers = {"Content-Type": "application/json"}

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"{self.status_code} Client Error")

    # Check if the URL is malformed
    parsed_url = urlparse(args[0])
    if not parsed_url.scheme:
        raise requests.exceptions.MissingSchema(
            "Invalid URL 'No_valid_url': No scheme supplied. Perhaps you meant http://No_valid_url?"
        )

    if args[0] == f"{URL}query/object-types":
        return MockResponse(object_types, 200)
    elif args[0] == f"{URL}query/namespace-array":
        return MockResponse(namespaces, 200)
    elif args[0] == f"{URL}query/objects-of-type":
        return MockResponse(objects_of_type, 200)
    elif args[0] == f"{URL}query/object-descendants":
        return MockResponse(descendants, 200)
    elif args[0] == f"{URL}query/object-ancestors":
        return MockResponse(ancestors, 200)

    return MockResponse(None, 404)


requests.get = Mock(side_effect=mocked_requests)
requests.post = Mock(side_effect=mocked_requests)


@pytest.fixture
def model_index():
    return ModelIndex(url=URL)


class AnyUrlModel(BaseModel):
    url: AnyUrl


class TestCaseModelIndex:

    @patch("pyprediktormapclient.model_index.ModelIndex.get_object_types")
    def setup_model_index_test_environment(self, mock_get_object_types):
        mock_get_object_types.return_value = object_types

    def test_init_without_auth_client(self, model_index):
        assert "Authorization" not in model_index.headers
        assert "Cookie" not in model_index.headers

    def test_init_with_auth_client_no_tokens(self):
        auth_client = Mock(spec=[])
        auth_client.token = None
        model = ModelIndex(url=URL, auth_client=auth_client)
        assert "Authorization" not in model.headers
        assert "Cookie" not in model.headers

    def test_init_with_auth_client_both_tokens(self):
        auth_client = Mock()
        auth_client.token = Mock()
        auth_client.token.session_token = "test_token"
        auth_client.session_token = "ory_session_token"
        model = ModelIndex(url=URL, auth_client=auth_client)
        assert model.headers["Authorization"] == "Bearer test_token"
        assert (
            model.headers["Cookie"] == "ory_kratos_session=ory_session_token"
        )

    def test_malformed_url(self):
        with pytest.raises(ValidationError):
            AnyUrlModel(url="not_an_url")

    def test_json_serial(self, model_index):

        dt = datetime(2023, 1, 1, 12, 0, 0)
        assert model_index.json_serial(dt) == "2023-01-01T12:00:00"

        d = date(2023, 1, 1)
        assert model_index.json_serial(d) == "2023-01-01"

        url = Url("http://example.com")
        assert model_index.json_serial(url) == "http://example.com/"

        with pytest.raises(TypeError):
            model_index.json_serial(set())

    def test_check_auth_client(self, model_index):
        model_index.auth_client = Mock()

        model_index.auth_client.token = Mock()
        model_index.auth_client.token.session_token = "new_token"
        content = {"error": {"code": 404}}
        model_index.check_auth_client(content)
        model_index.auth_client.request_new_ory_token.assert_called_once()
        assert model_index.headers["Authorization"] == "Bearer new_token"

        model_index.auth_client.token = None
        model_index.auth_client.request_new_ory_token = Mock()

        def side_effect():
            model_index.auth_client.token = Mock()
            model_index.auth_client.token.session_token = "new_token"

        model_index.auth_client.request_new_ory_token.side_effect = side_effect
        model_index.check_auth_client(content)
        model_index.auth_client.request_new_ory_token.assert_called_once()
        assert model_index.headers["Authorization"] == "Bearer new_token"

        content = {"ErrorMessage": "Some error"}
        with pytest.raises(RuntimeError, match="Some error"):
            model_index.check_auth_client(content)

    def test_get_namespace_array(self, model_index):
        result = model_index.get_namespace_array()
        assert result == namespaces

    def test_get_object_types(self, model_index):
        result = model_index.get_object_types()
        assert result == object_types

    def test_get_object_type_id_from_name(self, model_index):
        assert (
            model_index.get_object_type_id_from_name("IPVBaseCalculate")
            == "6:0:1029"
        )
        assert (
            model_index.get_object_type_id_from_name("NonExistentType") is None
        )

    def test_get_objects_of_type(self, model_index):
        assert (
            model_index.get_objects_of_type(type_name="IPVBaseCalculate")
            == objects_of_type
        )
        assert (
            model_index.get_objects_of_type(type_name="IPVBaseCalculate2")
            is None
        )

    def test_get_object_descendants(self, model_index):
        result = model_index.get_object_descendants(
            type_name="IPVBaseCalculate",
            ids=["Anything"],
            domain="PV_Assets",
        )
        assert result == descendants

        with pytest.raises(ValueError):
            model_index.get_object_descendants(
                type_name=None, ids=["Anything"], domain="PV_Assets"
            )

        with pytest.raises(ValueError):
            model_index.get_object_descendants(
                type_name="IPVBaseCalculate", ids=None, domain="PV_Assets"
            )

    def test_get_object_ancestors(self, model_index):
        result = model_index.get_object_ancestors(
            type_name="IPVBaseCalculate",
            ids=["Anything"],
            domain="PV_Assets",
        )
        assert result == ancestors

        with pytest.raises(ValueError):
            model_index.get_object_ancestors(
                type_name=None, ids=["Anything"], domain="PV_Assets"
            )

        with pytest.raises(ValueError):
            model_index.get_object_ancestors(
                type_name="IPVBaseCalculate", ids=None, domain="PV_Assets"
            )


if __name__ == "__main__":
    unittest.main()
