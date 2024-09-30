import logging
import re
from typing import Any, List

import pandas as pd
from pydantic import validate_call

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class AnalyticsHelper:
    """Your data as a Pandas DataFrame, but with a specific formatting and some
    nifty functions. Put the data from ModelIndex in here and move further on
    by adding data and from other ModelIndex calls or live or historical data
    from OPC UA.

    Columns in the normalizes dataframe are:

    - Id
    - Name
    - Type
    - Props
        - DisplayName
        - Value
    - Vars
        - DisplayName
        - Id

    Args:
        input (List): The return from a ModelIndex call function

    Attributes:
        dataframe (pandas.DataFrame): The normalized dataframe

    Returns:
        An instance of the class with some resources and attributes

    Todo:
        * Input checks for nodeIds in variables that requires format int:int:string
    """

    ID_PATTERN = r"^\d+:\d+:\S+$"

    @validate_call
    def __init__(self, input: List):
        self.dataframe = pd.DataFrame(
            input
        )  # Create a DataFrame from the input
        self.normalize()

    def normalize(self):
        """Normalize column names in the `dataframe` class attribute. Different
        ModelIndex calls has different column names but this will be normalized
        with this function according to the class docs.

        Returns:
            Nothing, but normalizes the instance "dataframe"
        """

        # Remove "Subtype"
        if "Subtype" in self.dataframe.columns:
            self.dataframe.drop("Subtype", inplace=True, axis=1)

        # Remove "ObjectId"
        if "ObjectId" in self.dataframe.columns:
            self.dataframe.drop("ObjectId", inplace=True, axis=1)

        # Remove "ObjectId"
        if "ObjectName" in self.dataframe.columns:
            self.dataframe.drop("ObjectName", inplace=True, axis=1)

        # Check if the content is from get_objects_of_type
        if "DisplayName" in self.dataframe.columns:
            self.dataframe.rename(
                columns={"DisplayName": "Name"}, inplace=True
            )

        # Check if the content is from object-descendants
        if "DescendantId" in self.dataframe.columns:
            self.dataframe.rename(
                columns={
                    "DescendantId": "Id",
                    "DescendantName": "Name",
                    "DescendantType": "Type",
                },
                inplace=True,
            )

        # Check if the content is from object-ancestors
        if "AncestorId" in self.dataframe.columns:
            self.dataframe.rename(
                columns={
                    "AncestorId": "Id",
                    "AncestorName": "Name",
                    "AncestorType": "Type",
                },
                inplace=True,
            )

        # Now check to see if all needed columns are there, else set to None
        for required_key in ["Id", "Name", "Vars", "Props"]:
            if required_key not in self.dataframe:
                self.dataframe = None
                return

    @validate_call
    def namespaces_as_list(self, list_of_dicts: List) -> List:
        """Takes the output of a get_namespace_array() request from ModelIndex
        and generates a list of strings that can be used for the OPC UA Values
        API.

        Args:
            list_of_dicts (List): A list in of dicts like [{'Idx': 0, 'Uri': 'http://opcfoundation.org/UA/'}, etc]

        Returns:
            List: A list of strings containing the URIs
        """
        new_list = []
        for item in list_of_dicts:
            if "Uri" in item:
                new_list.append(item["Uri"])

        return new_list

    @validate_call
    def split_id(self, id: Any) -> dict:

        if not re.match(self.ID_PATTERN, id):
            raise ValueError("Invalid id format")

        id_split = id.split(":")
        return {
            "Id": id_split[2],
            "Namespace": int(id_split[0]),
            "IdType": int(id_split[1]),
        }

    def list_of_ids(self) -> list:
        """Extracts the values in the column "Id" to a list of unique IDs.

        Returns:
            list: Unique IDs
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return []

        # Get the content of the column Id as a list, loop it through
        # the set function to remove duplicates and then back to a list
        return list(set(self.dataframe["Id"].to_list()))

    def list_of_names(self) -> list:
        """Extracts the values in the column "Name" to a list of unique names.

        Returns:
            list: Unique names
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return []

        # Get the content of the column Name as a list, loop it through
        # the set function to remove duplicates and then back to a list
        return list(set(self.dataframe["Name"].to_list()))

    def list_of_types(self) -> list:
        """Extracts the values in the column "Type" to a list of unique types.

        Returns:
            list: Unique types
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return []

        # Get the content of the column Type as a list, loop it through
        # the set function to remove duplicates and then back to a list
        return list(set(self.dataframe["Type"].to_list()))

    def list_of_variable_names(self) -> list:
        """Explodes the content of the column "Vars" and extracts the values
        from DisplayName into a list of unique values.

        Returns:
            list: Unique variable names
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return []

        # Check that the Vars column contains pd.Series content
        if not isinstance(self.dataframe["Vars"][0], list):
            return []

        vars_set = set([])
        # Loop through the Vars column and add DisplayName to the vars_set
        for i in self.dataframe["Vars"].array:
            for a in i:
                vars_set.add(a["DisplayName"])

        return list(vars_set)

    def properties_as_dataframe(self) -> pd.DataFrame:
        """Explodes the column "Props" into a new dataframe. Column names will
        be.

        - Id (same as from the original dataframe)
        - Name (same as from the original dataframe)
        - Type (same as from the original dataframe)
        - Property (from the exploded value DisplayName)
        - Value (from the exploded value Value)

        Returns:
            pandas.DataFrame: A new dataframe with all properties as individual rows
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if the Props column contains pd.Series data
        if not isinstance(self.dataframe["Props"][0], list):
            return None

        # Explode will add a row for every series in the Prop column
        propery_frame = self.dataframe.explode("Props")
        # Remove Vars
        propery_frame.drop(columns=["Vars"], inplace=True)
        # Add Property and Value vcolumns
        propery_frame["Property"] = ""
        propery_frame["Value"] = ""

        # Add new columns
        propery_frame[["Property", "Value"]] = propery_frame["Props"].apply(
            lambda x: pd.Series(
                {"Property": x["DisplayName"], "Value": x["DisplayName"]}
            )
        )

        # Remove original Props column
        propery_frame.drop(columns=["Props"], inplace=True)

        return propery_frame

    def variables_as_dataframe(self) -> pd.DataFrame:
        """Explodes the column "Vars" into a new dataframe. Column names will
        be.

        - Id (same as from the original dataframe)
        - Name (same as from the original dataframe)
        - Type (same as from the original dataframe)
        - VariableId (from the exploded value Id)
        - VariableName (from the exploded value DisplayName)

        Returns:
            pandas.DataFrame: A new dataframe with all variables as individual rows
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if the Vars column contains list content
        if not isinstance(self.dataframe["Vars"][0], list):
            return None

        # Explode will add a row for every series in the Prop column
        variables_frame = self.dataframe.explode("Vars")
        # Remove the Props column
        variables_frame.drop(columns=["Props"], inplace=True)

        # Add new columns
        variables_frame[
            ["VariableId", "VariableName", "VariableIdSplit"]
        ] = variables_frame["Vars"].apply(
            lambda x: pd.Series(
                {
                    "VariableId": x["Id"],
                    "VariableName": x["DisplayName"],
                    "VariableIdSplit": self.split_id(x["Id"]),
                }
            )
        )

        # Remove the original Vars column
        variables_frame.drop(columns=["Vars"], inplace=True)

        return variables_frame

    def variables_as_list(self, include_only: List = []) -> List:
        """Extracts variables as a list. If there are names listed in the
        include_only argument, only variables matching that name will be
        encluded.

        Args:
            include_only (list): A list of variable names (str) that should be included

        Returns:
            list: Unique types
        """
        variable_dataframe = self.variables_as_dataframe()
        # If there are any items in the include_only list, include only them
        if len(include_only) > 0:
            variable_dataframe = variable_dataframe[
                variable_dataframe.VariableName.isin(include_only)
            ]
        return variable_dataframe["VariableIdSplit"].to_list()
