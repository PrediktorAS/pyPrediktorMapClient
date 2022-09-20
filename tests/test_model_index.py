import requests
import os
import sys
import unittest
from unittest import mock
from pandas.testing import assert_frame_equal

from pyprediktormapclient.model_index import ModelIndex

URL = "http://someserver.somedomain.com/v1/"
object_types = [
            {
                "Id": "6:0:1029",
                "DisplayName": "IPVBaseCalculate",
                "BrowseName": "IPVBaseCalculate",
                "Props": [],
                "Vars": []
            }]

namespaces = [
                {
                    "Idx": 0,
                    "Uri": "http://opcfoundation.org/UA/"
                }]

objects_of_type = [
                {
                    "Id": "3:1:SSO.EG-AS",
                    "Type": "6:0:1009",
                    "Subtype": "6:0:1009",
                    "DisplayName": "EG-AS",
                    "Props": [
                    {
                        "DisplayName": "GPSLatitude",
                        "Value": "24.44018"
                    }
                    ],
                    "Vars": [
                    {
                        "DisplayName": "Alarm.CommLossPlantDevice",
                        "Id": "3:1:SSO.EG-AS.Signals.Alarm.CommLossPlantDevice"
                    }
                    ]
                }]

# This method will be used by the mock to replace requests.get
def mocked_requests(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.raise_for_status=mock.Mock(return_value=False)

        def json(self):
            return self.json_data

    print(args)
    if args[0] == f"{URL}query/object-types":
        return MockResponse(object_types, 200)
    elif args[0] == f"{URL}query/namespace-array":
        return MockResponse(namespaces, 200)
    elif args[0] == f"{URL}query/objects-of-type":
        return MockResponse(objects_of_type, 200)

    return MockResponse(None, 404)

# Our test case class
class ModelIndexTestCase(unittest.TestCase):

    @mock.patch('requests.get', side_effect=mocked_requests)
    def test_get_object_types_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        result = model.get_object_types(return_format="json")
        assert result == object_types

    @mock.patch('requests.get', side_effect=mocked_requests)
    def test_get_object_types_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        result_json = model.get_object_types(return_format="json")
        result = model.get_object_types(return_format="dataframe")
        assert_frame_equal(result, model.as_dataframe(result_json))

    @mock.patch('requests.get', side_effect=mocked_requests)
    def test_get_namespace_array_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        result = model.get_namespace_array(return_format="json")
        assert result == namespaces

    @mock.patch('requests.get', side_effect=mocked_requests)
    def test_get_namespace_array_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        result_json = model.get_namespace_array(return_format="json")
        result = model.get_namespace_array(return_format="dataframe")
        assert_frame_equal(result, model.as_dataframe(result_json))

    @mock.patch('requests.get', side_effect=mocked_requests)
    def test_get_object_of_type_as_json(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch('requests.post', side_effect=mocked_requests):
            result = model.get_objects_of_type(type_name='IPVBaseCalculate', return_format="json")
            assert result == objects_of_type

    @mock.patch('requests.get', side_effect=mocked_requests)
    def test_get_object_of_type_as_dataframe(self, mock_get):
        model = ModelIndex(url=URL)
        with mock.patch('requests.post', side_effect=mocked_requests):
            result_json = model.get_objects_of_type(type_name='IPVBaseCalculate', return_format="json")
            result = model.get_objects_of_type(type_name='IPVBaseCalculate', return_format="dataframe")
            assert_frame_equal(result, model.as_dataframe(result_json))


if __name__ == '__main__':
    unittest.main()
