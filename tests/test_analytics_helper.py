import unittest
import json
import pandas as pd

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
            {"Id": "SomeId", "DisplayName": "SomeName"},
            {"Id": "SomeId2", "DisplayName": "SomeName2"},
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
            {"Id": "SomeId", "DisplayName": "SomeName"},
            {"Id": "SomeId2", "DisplayName": "SomeName2"},
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
        assert "SomeName" in result.list_of_variables()
        assert "SomeName2" in result.list_of_variables()
        assert "Property" in result.properties_as_dataframe()
        assert "Property1" in result.properties_as_dataframe()["Property"].to_list()
        assert "VariableName" in result.variables_as_dataframe()
        assert "SomeName" in result.variables_as_dataframe()["VariableName"].to_list()

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
        assert result.list_of_variables() == []

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
        assert result.list_of_variables() == []
        assert result.properties_as_dataframe() is None
        assert result.variables_as_dataframe() is None


if __name__ == "__main__":
    unittest.main()
