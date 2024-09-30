import unittest

import pandas as pd
import pytest

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


@pytest.fixture
def proper_helper():
    return AnalyticsHelper(proper_json)


@pytest.fixture
def descendant_helper():
    return AnalyticsHelper(descendant_json)


@pytest.fixture
def ancestor_helper():
    return AnalyticsHelper(ancestor_json)


@pytest.fixture
def faulty_helper():
    return AnalyticsHelper(faulty_json)


class TestCaseAnalyticsHelper:
    def test_analytics_helper_initialization_success(self, proper_helper):
        assert isinstance(proper_helper.dataframe, pd.DataFrame)
        assert "Subtype" not in proper_helper.dataframe
        assert "ObjectId" not in proper_helper.dataframe
        assert "ObjectName" not in proper_helper.dataframe
        assert proper_helper.list_of_ids() == ["id1"]
        assert proper_helper.list_of_names() == ["SomeName"]
        assert proper_helper.list_of_types() == ["SomeType"]
        assert "SomeName" in proper_helper.list_of_variable_names()
        assert "SomeName2" in proper_helper.list_of_variable_names()
        assert "Property" in proper_helper.properties_as_dataframe()
        assert (
            "Property1"
            in proper_helper.properties_as_dataframe()["Property"].to_list()
        )
        assert "VariableName" in proper_helper.variables_as_dataframe()
        assert (
            "SomeName"
            in proper_helper.variables_as_dataframe()["VariableName"].to_list()
        )

    def test_split_id_success(self, proper_helper):
        result = proper_helper.split_id("1:2:TEXT")
        assert result["Id"] == "TEXT"
        assert result["Namespace"] == 1
        assert result["IdType"] == 2

    def test_split_id_failure(self, proper_helper):
        with pytest.raises(ValueError):
            proper_helper.split_id("TEXT:TEXT:TEXT")

    def test_analytics_helper_descendants_success(self, descendant_helper):
        assert isinstance(descendant_helper.dataframe, pd.DataFrame)
        assert "DescendantId" not in descendant_helper.dataframe
        assert "DescendantType" not in descendant_helper.dataframe
        assert "DescendantName" not in descendant_helper.dataframe

    def test_analytics_helper_ancestor_success(self, ancestor_helper):
        assert isinstance(ancestor_helper.dataframe, pd.DataFrame)
        assert "AncestorId" not in ancestor_helper.dataframe
        assert "AncestorType" not in ancestor_helper.dataframe
        assert "AncestorName" not in ancestor_helper.dataframe

    def test_analytics_helper_missing_vars_list(self, ancestor_helper):
        assert ancestor_helper.list_of_variable_names() == []

    def test_analytics_helper_missing_props(self, ancestor_helper):
        assert ancestor_helper.properties_as_dataframe() is None

    def test_analytics_helper_missing_vars(self, ancestor_helper):
        assert ancestor_helper.variables_as_dataframe() is None

    def test_analytics_helper_failure(self, faulty_helper):
        assert faulty_helper.dataframe is None
        assert faulty_helper.list_of_ids() == []
        assert faulty_helper.list_of_names() == []
        assert faulty_helper.list_of_types() == []
        assert faulty_helper.list_of_variable_names() == []
        assert faulty_helper.properties_as_dataframe() is None
        assert faulty_helper.variables_as_dataframe() is None

    def test_analytics_helper_variables_as_list(self, proper_helper):
        ids = proper_helper.variables_as_list()
        assert "Id" in ids[0].keys()
        assert "Namespace" in ids[0].keys()
        assert "IdType" in ids[0].keys()
        assert len(ids) == 2

    def test_analytics_helper_variables_as_list_with_include_only(
        self, proper_helper
    ):
        ids = proper_helper.variables_as_list(include_only=["SomeName2"])
        assert len(ids) == 1
        ids2 = proper_helper.variables_as_list(include_only=["SomeRandomName"])
        assert len(ids2) == 0

    def test_namespaces_as_list_successful(self, proper_helper):
        nslist = proper_helper.namespaces_as_list(namespace_array)
        assert len(nslist) == 7
        assert nslist[0] == "http://opcfoundation.org/UA/"

    def test_namespaces_as_list_empty(self, proper_helper):
        nslist = proper_helper.namespaces_as_list(["CrappyItem"])
        assert len(nslist) == 0


if __name__ == "__main__":
    unittest.main()
