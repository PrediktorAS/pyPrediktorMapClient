import unittest
import pandas as pd

from pyprediktormapclient.analytics_helper import AnalyticsHelper

proper_json = [
    {
        "Id": "id1",
        "Type": "SomeType",
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

faulty_json = [{"Rubbish": "MoreRubbish", "ShitIn": "ShitOut"}]

# Our test case class
class AnalyticsHelperTestCase(unittest.TestCase):
    def test_analytics_helper_initialization_success(self):
        result = AnalyticsHelper(proper_json)
        assert isinstance(result.dataframe, pd.DataFrame)
        assert result.list_of_ids() == ["id1"]
        assert result.list_of_names() == ["SomeName"]
        assert result.list_of_types() == ["SomeType"]
        assert "SomeName" in result.list_of_variables()
        assert "SomeName2" in result.list_of_variables()
        assert "Property" in result.properties_as_dataframe()
        assert "Property1" in result.properties_as_dataframe()["Property"].to_list()
        assert "VariableName" in result.variables_as_dataframe()
        assert "SomeName" in result.variables_as_dataframe()["VariableName"].to_list()

    def test_analytics_helper_failure(self):
        result = AnalyticsHelper(faulty_json)
        assert result.dataframe is None
        assert result.list_of_ids() is None
        assert result.list_of_names() is None
        assert result.list_of_types() is None
        assert result.list_of_variables() is None
        assert result.properties_as_dataframe() is None
        assert result.variables_as_dataframe() is None


if __name__ == "__main__":
    unittest.main()
