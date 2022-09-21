import requests
import unittest
from unittest import mock
from pandas.testing import assert_frame_equal

from pyprediktormapclient.model_index import ModelIndex
from pyprediktormapclient.shared import normalize_as_dataframe

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
            self.raise_for_status = mock.Mock(return_value=False)

        def json(self):
            return self.json_data

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


# Our test case class
class ModelIndexTestCase(unittest.TestCase):
    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_types_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        result = model.get_object_types(return_format="json")
        assert result == object_types

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_types_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        result_json = model.get_object_types(return_format="json")
        result = model.get_object_types(return_format="dataframe")
        assert_frame_equal(result, normalize_as_dataframe(result_json))

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_namespace_array_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        result = model.get_namespace_array(return_format="json")
        assert result == namespaces

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_namespace_array_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        result_json = model.get_namespace_array(return_format="json")
        result = model.get_namespace_array(return_format="dataframe")
        assert_frame_equal(result, normalize_as_dataframe(result_json))

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_of_type_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_objects_of_type(
                type_name="IPVBaseCalculate", return_format="json"
            )
            assert result == objects_of_type

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_of_type_as_json_with_wrong_type(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_objects_of_type(
                type_name="IPVBaseCalculate2", return_format="json"
            )
            assert result == None

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_of_type_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result_json = model.get_objects_of_type(
                type_name="IPVBaseCalculate", return_format="json"
            )
            result = model.get_objects_of_type(
                type_name="IPVBaseCalculate", return_format="dataframe"
            )
            assert_frame_equal(result, normalize_as_dataframe(result_json))

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_descendants_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_descendants(
                type_name="IPVBaseCalculate", ids=["Anything"], domain="PV_Assets"
            )
            assert result == descendants

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_descendants_with_no_id(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_descendants(
                type_name=None, ids=["Anything"], domain="PV_Assets"
            )
            assert result == None

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_descendants_with_no_ids(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_descendants(
                type_name="IPVBaseCalculate", ids=None, domain="PV_Assets"
            )
            assert result == None

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_descendants_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result_json = model.get_object_descendants(
                type_name="IPVBaseCalculate", ids=["Anything"], domain="PV_Assets"
            )
            result_dataframe = model.get_object_descendants(
                type_name="IPVBaseCalculate",
                ids=["Anything"],
                domain="PV_Assets",
                return_format="dataframe",
            )
            assert_frame_equal(result_dataframe, normalize_as_dataframe(result_json))

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_ancestors_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_ancestors(
                type_name="IPVBaseCalculate", ids=["Anything"], domain="PV_Assets"
            )
            assert result == ancestors

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_ancestors_with_no_id(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_ancestors(
                type_name=None, ids=["Anything"], domain="PV_Assets"
            )
            assert result == None

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_ancestors_with_no_ids(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result = model.get_object_ancestors(
                type_name="IPVBaseCalculate", ids=None, domain="PV_Assets"
            )
            assert result == None

    @mock.patch("requests.get", side_effect=mocked_requests)
    def test_get_object_ancestors_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch("requests.post", side_effect=mocked_requests):
            result_json = model.get_object_ancestors(
                type_name="IPVBaseCalculate", ids=["Anything"], domain="PV_Assets"
            )
            result_dataframe = model.get_object_ancestors(
                type_name="IPVBaseCalculate",
                ids=["Anything"],
                domain="PV_Assets",
                return_format="dataframe",
            )
            assert_frame_equal(result_dataframe, normalize_as_dataframe(result_json))


if __name__ == "__main__":
    unittest.main()
