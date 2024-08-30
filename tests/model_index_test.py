import pytest
import unittest
from unittest import mock
from pydantic import ValidationError, BaseModel, AnyUrl
from pyprediktormapclient.model_index import ModelIndex
from datetime import datetime, date
from pydantic_core import Url
import requests

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


# This method will be used by the mock to replace requests.get
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


class AnyUrlModel(BaseModel):
    url: AnyUrl


class TestCaseModelIndex(unittest.TestCase):

    @mock.patch("requests.get", side_effect=mocked_requests)
    @mock.patch("pyprediktormapclient.model_index.ModelIndex.get_object_types")
    def test_init_variations(self, mock_get_object_types, mock_get):
        mock_get_object_types.return_value = object_types

        # Without auth_client
        model = ModelIndex(url=URL)
        assert "Authorization" not in model.headers
        assert "Cookie" not in model.headers

        # With auth_client, no token, no session_token
        auth_client = mock.Mock(spec=[])
        auth_client.token = None
        model = ModelIndex(url=URL, auth_client=auth_client)
        assert "Authorization" not in model.headers
        assert "Cookie" not in model.headers

        # With auth_client, token, and session_token
        auth_client = mock.Mock()
        auth_client.token = mock.Mock()
        auth_client.token.session_token = "test_token"
        auth_client.session_token = "ory_session_token"
        model = ModelIndex(url=URL, auth_client=auth_client)
        assert model.headers["Authorization"] == "Bearer test_token"
        assert (
            model.headers["Cookie"] == "ory_kratos_session=ory_session_token"
        )

        # With auth_client, no token, with session_token
        auth_client.token = None
        model = ModelIndex(url=URL, auth_client=auth_client)
        assert "Authorization" not in model.headers
        assert (
            model.headers["Cookie"] == "ory_kratos_session=ory_session_token"
        )

        # With session
        session = requests.Session()
        model = ModelIndex(url=URL, session=session)
        assert model.session == session

    def test_malformed_url(self):
        with pytest.raises(ValidationError):
            AnyUrlModel(url="not_an_url")

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_json_serial(self, mock_get):
        model = ModelIndex(url=URL)

        dt = datetime(2023, 1, 1, 12, 0, 0)
        assert model.json_serial(dt) == "2023-01-01T12:00:00"

        d = date(2023, 1, 1)
        assert model.json_serial(d) == "2023-01-01"

        url = Url("http://example.com")
        assert model.json_serial(url) == "http://example.com/"

        with pytest.raises(TypeError):
            model.json_serial(set())

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_check_auth_client(self, mock_get):
        model = ModelIndex(url=URL)
        model.auth_client = mock.Mock()

        model.auth_client.token = mock.Mock()
        model.auth_client.token.session_token = "new_token"
        content = {"error": {"code": 404}}
        model.check_auth_client(content)
        model.auth_client.request_new_ory_token.assert_called_once()
        assert model.headers["Authorization"] == "Bearer new_token"

        model.auth_client.token = None
        model.auth_client.request_new_ory_token = mock.Mock()

        def side_effect():
            model.auth_client.token = mock.Mock()
            model.auth_client.token.session_token = "new_token"

        model.auth_client.request_new_ory_token.side_effect = side_effect
        model.check_auth_client(content)
        model.auth_client.request_new_ory_token.assert_called_once()
        assert model.headers["Authorization"] == "Bearer new_token"

        content = {"ErrorMessage": "Some error"}
        with pytest.raises(RuntimeError, match="Some error"):
            model.check_auth_client(content)

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_namespace_array(self, mock_get):
        model = ModelIndex(url=URL)
        result = model.get_namespace_array()
        assert result == namespaces

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_types(self, mock_get):
        model = ModelIndex(url=URL)
        result = model.get_object_types()
        assert result == object_types

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_type_id_from_name(self, mock_get):
        model = ModelIndex(url=URL)
        assert (
            model.get_object_type_id_from_name("IPVBaseCalculate")
            == "6:0:1029"
        )
        assert model.get_object_type_id_from_name("NonExistentType") is None

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_objects_of_type(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            assert (
                model.get_objects_of_type(type_name="IPVBaseCalculate")
                == objects_of_type
            )
            assert (
                model.get_objects_of_type(type_name="IPVBaseCalculate2")
                is None
            )

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_descendants(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_descendants(
                type_name="IPVBaseCalculate",
                ids=["Anything"],
                domain="PV_Assets",
            )
            assert result == descendants

        with self.assertRaises(ValueError):
            model.get_object_descendants(
                type_name=None, ids=["Anything"], domain="PV_Assets"
            )

        with self.assertRaises(ValueError):
            model.get_object_descendants(
                type_name="IPVBaseCalculate", ids=None, domain="PV_Assets"
            )

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_ancestors(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_ancestors(
                type_name="IPVBaseCalculate",
                ids=["Anything"],
                domain="PV_Assets",
            )
            assert result == ancestors

        with self.assertRaises(ValueError):
            model.get_object_ancestors(
                type_name=None, ids=["Anything"], domain="PV_Assets"
            )

        with self.assertRaises(ValueError):
            model.get_object_ancestors(
                type_name="IPVBaseCalculate", ids=None, domain="PV_Assets"
            )


if __name__ == "__main__":
    unittest.main()
