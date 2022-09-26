import pandas as pd
import json


class AnalyticsHelper:
    """Your data as a Pandas DataFrame, but with a specific formatting
    and some nifty functions. Put the data from ModelIndex in here
    and move further on by adding data and from other ModelIndex calls
    or live or historical data from OPC UA.

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

    Returns:
        An instance of the class with some resources. You can access the dataframe directly as "dataframe"
    """

    def __init__(self, initial_data: str):
        self.dataframe = pd.DataFrame(initial_data)
        self.normalize()

    def normalize(self):
        """Normalize column names in the dataframe. Different ModelIndex calls has different column names
        but this will be normalized with this function according to the class docs.

        Columns in the dataframe are:
        - Id
        - Name
        - Type
        - Props
            - DisplayName
            - Value
        - Vars
            - DisplayName
            - Id

        Returns:
            Nothing, but nomralizes the instance "dataframe"
        """

        # Check if there is an Id column

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
            self.dataframe.rename(columns={"DisplayName": "Name"}, inplace=True)

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
        needed_columns = ["Id", "Name", "Type", "Vars", "Props"]
        for needed_column in needed_columns:
            if not needed_column in self.dataframe:
                self.dataframe = None
                return

    def list_of_ids(self) -> list:
        """Extracts the values in the column "Id" to a list of unique IDs

        Returns:
            list of unique IDs
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if there is an Id column present
        if "Id" in self.dataframe.columns:
            # Get the content of the column Id as a list, loop it through
            # the set function to remove duplicates and then back to a list
            return list(set(self.dataframe["Id"].to_list()))

        return []

    def list_of_names(self) -> list:
        """Extracts the values in the column "Name" to a list of unique names

        Returns:
            list of unique names
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if there is a Name column
        if "Name" in self.dataframe.columns:
            # Get the content of the column Name as a list, loop it through
            # the set function to remove duplicates and then back to a list
            return list(set(self.dataframe["Name"].to_list()))

        return []

    def list_of_types(self) -> list:
        """Extracts the values in the column "Type" to a list of unique types

        Returns:
            list of unique types
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if there is a Type column
        if "Type" in self.dataframe.columns:
            # Get the content of the column Type as a list, loop it through
            # the set function to remove duplicates and then back to a list
            return list(set(self.dataframe["Type"].to_list()))

        return []

    def list_of_variables(self) -> list:
        """Explodes the content of the column "Vars" and extracts the values
        from DisplayName into a list of unique values

        Returns:
            list of unique variable names
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if there is a Vars column
        if not "Vars" in self.dataframe:
            return []

        # Check that the Vars column contains pd.Series content
        if not isinstance(self.dataframe["Vars"], pd.Series):
            return []

        vars_set = set([])
        # Loop through the Vars column and add DisplayName to the vars_set
        for i in self.dataframe["Vars"].array:
            for a in i:
                vars_set.add(a["DisplayName"])

        return list(vars_set)

    def properties_as_dataframe(self) -> pd.DataFrame:
        """Explodes the column "Props" into a new dataframe. Column names will be
        - Id (same as from the original dataframe)
        - Name (same as from the original dataframe)
        - Type (same as from the original dataframe)
        - Property (from the exploded value DisplayName)
        - Value (from the exploded value Value)

        Returns:
            a new dataframe with all properties as individual rows
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if there is a Props column
        if not "Props" in self.dataframe:
            return None

        # Check if the Props column contains pd.Series data
        if not isinstance(self.dataframe["Props"], pd.Series):
            return None

        # Explode will add a row for every series in the Prop column
        propery_frame = self.dataframe.explode("Props")
        # Remove Vars
        propery_frame.drop(columns=["Vars"], inplace=True)
        # Add Property and Value vcolumns
        propery_frame["Property"] = ""
        propery_frame["Value"] = ""
        # Iterate over the rows and add to the new columns
        for index, row in propery_frame.iterrows():
            row["Property"] = row["Props"]["DisplayName"]
            row["Value"] = row["Props"]["Value"]
        # Remove original Props column
        propery_frame.drop(columns=["Props"], inplace=True)

        return propery_frame

    def variables_as_dataframe(self) -> pd.DataFrame:
        """Explodes the column "Vars" into a new dataframe. Column names will be
        - Id (same as from the original dataframe)
        - Name (same as from the original dataframe)
        - Type (same as from the original dataframe)
        - VariableId (from the exploded value Id)
        - VariableName (from the exploded value DisplayName)

        Returns:
            a new dataframe with all variables as individual rows
        """

        # Return if dataframe is None
        if self.dataframe is None:
            return None

        # Check if there is a Vars column
        if not "Vars" in self.dataframe:
            return None

        # Check if the Vars column contains pd.Series content
        if not isinstance(self.dataframe["Vars"], pd.Series):
            return None

        # Explode will add a row for every series in the Prop column
        variables_frame = self.dataframe.explode("Vars")
        # Remove the Props column
        variables_frame.drop(columns=["Props"], inplace=True)
        # Add VariableId and VariableName columns
        variables_frame["VariableId"] = ""
        variables_frame["VariableName"] = ""
        # Iterate over the rows and add to the new columns
        for index, row in variables_frame.iterrows():
            row["VariableId"] = row["Vars"]["Id"]
            row["VariableName"] = row["Vars"]["DisplayName"]
        # Remove the original Vars column
        variables_frame.drop(columns=["Vars"], inplace=True)

        return variables_frame
