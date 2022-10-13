import unittest
import json
import pandas as pd
import pytest
from pydantic import ValidationError

from pyprediktormapclient.analytics_helper import AnalyticsHelper

proper_json = [
    {
        "Id": "id1",
        "Type": "SomeType",
        "Subtype": "ShouldBeDeleted",
        "ObjectId": "ShouldBeDeleted",
        "ObjectName": "ShouldBeDeleted",
        "DisplayName": "SomeName",
        "Props": [
            {"DisplayName": "Property1", "Value": "Value1"},
            {"DisplayName": "Property2", "Value": "Value2"},
        ],
        "Vars": [
            {"Id": "1:1:SomeId", "DisplayName": "SomeName"},
            {"Id": "2:2:SomeId2", "DisplayName": "SomeName2"},
        ],
    }
]

descendant_json = [
    {
        "DescendantId": "id1",
        "DescendantType": "SomeType",
        "Subtype": "ShouldBeDeleted",
        "ObjectId": "ShouldBeDeleted",
        "ObjectName": "ShouldBeDeleted",
        "DescendantName": "SomeName",
        "Props": [
            {"DisplayName": "Property1", "Value": "Value1"},
            {"DisplayName": "Property2", "Value": "Value2"},
        ],
        "Vars": [
            {"Id": "1:1:SomeId", "DisplayName": "SomeName"},
            {"Id": "2:2:SomeId2", "DisplayName": "SomeName2"},
        ],
    }
]

ancestor_json = [
    {
        "AncestorId": "id1",
        "AncestorType": "SomeType",
        "Subtype": "ShouldBeDeleted",
        "ObjectId": "ShouldBeDeleted",
        "ObjectName": "ShouldBeDeleted",
        "AncestorName": "SomeName",
        "Props": "This should be a dict",
        "Vars": "This should be a dict",
    }
]


faulty_json = [{"Rubbish": "MoreRubbish", "ShitIn": "ShitOut"}]

namespace_array = [
    {"Idx": 0, "Uri": "http://opcfoundation.org/UA/"},
    {"Idx": 1, "Uri": "http://prediktor.no/apis/ua/"},
    {"Idx": 2, "Uri": "urn:prediktor:UAA-W2022-01:Scatec"},
    {"Idx": 3, "Uri": "http://scatecsolar.com/EG-AS"},
    {"Idx": 4, "Uri": "http://scatecsolar.com/Enterprise"},
    {"Idx": 5, "Uri": "http://scatecsolar.com/JO-GL"},
    {"Idx": 6, "Uri": "http://prediktor.no/PVTypes/"},
]

# Our test case class
class AnalyticsHelperTestCase(unittest.TestCase):
    def test_analytics_helper_initialization_success(self):
        result = AnalyticsHelper(proper_json)
        assert isinstance(result.dataframe, pd.DataFrame)
        assert "Subtype" not in result.dataframe
        assert "ObjectId" not in result.dataframe
        assert "ObjectName" not in result.dataframe
        assert result.list_of_ids() == ["id1"]
        assert result.list_of_names() == ["SomeName"]
        assert result.list_of_types() == ["SomeType"]
        assert "SomeName" in result.list_of_variable_names()
        assert "SomeName2" in result.list_of_variable_names()
        assert "Property" in result.properties_as_dataframe()
        assert "Property1" in result.properties_as_dataframe()["Property"].to_list()
        assert "VariableName" in result.variables_as_dataframe()
        assert "SomeName" in result.variables_as_dataframe()["VariableName"].to_list()

    def test_split_id_success(self):
        instance = AnalyticsHelper(proper_json)
        result = instance.split_id("1:2:TEXT")
        assert result["Id"] == "TEXT"
        assert result["Namespace"] == 1
        assert result["IdType"] == 2

    def test_split_id_failure(self):
        result = AnalyticsHelper(proper_json)
        with pytest.raises(ValidationError):
            result.split_id("TEXT:TEXT:TEXT")

    def test_analytics_helper_descendants_success(self):
        result = AnalyticsHelper(descendant_json)
        assert isinstance(result.dataframe, pd.DataFrame)
        assert "DescendantId" not in result.dataframe
        assert "DescendantType" not in result.dataframe
        assert "DescendantName" not in result.dataframe

    def test_analytics_helper_ancestor_success(self):
        result = AnalyticsHelper(ancestor_json)
        assert isinstance(result.dataframe, pd.DataFrame)
        assert "AncestorId" not in result.dataframe
        assert "AncestorType" not in result.dataframe
        assert "AncestorName" not in result.dataframe

    def test_analytics_helper_missing_vars_list(self):
        result = AnalyticsHelper(ancestor_json)
        assert result.list_of_variable_names() == []

    def test_analytics_helper_missing_props(self):
        result = AnalyticsHelper(ancestor_json)
        assert result.properties_as_dataframe() is None

    def test_analytics_helper_missing_vars(self):
        result = AnalyticsHelper(ancestor_json)
        assert result.variables_as_dataframe() is None

    def test_analytics_helper_failure(self):
        result = AnalyticsHelper(faulty_json)
        assert result.dataframe is None
        assert result.list_of_ids() == []
        assert result.list_of_names() == []
        assert result.list_of_types() == []
        assert result.list_of_variable_names() == []
        assert result.properties_as_dataframe() is None
        assert result.variables_as_dataframe() is None

    def test_analytics_helper_variables_as_list(self):
        result = AnalyticsHelper(proper_json)
        ids = result.variables_as_list()
        assert "Id" in ids[0].keys()
        assert "Namespace" in ids[0].keys()
        assert "IdType" in ids[0].keys()
        assert len(ids) == 2

    def test_analytics_helper_variables_as_list_with_include_only(self):
        result = AnalyticsHelper(proper_json)
        ids = result.variables_as_list(include_only=["SomeName2"])
        assert len(ids) == 1
        ids2 = result.variables_as_list(include_only=["SomeRandomName"])
        assert len(ids2) == 0

    def test_namespaces_as_list_successful(self):
        result = AnalyticsHelper(proper_json)
        nslist = result.namespaces_as_list(namespace_array)
        assert len(nslist) == 7
        assert nslist[0] == "http://opcfoundation.org/UA/"

    def test_namespaces_as_list_empty(self):
        result = AnalyticsHelper(proper_json)
        nslist = result.namespaces_as_list(["CrappyItem"])
        assert len(nslist) == 0


if __name__ == "__main__":
    unittest.main()
