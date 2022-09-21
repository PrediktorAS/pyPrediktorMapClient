import requests
import pandas as pd


def request_from_api(
    rest_url: str, method: str, endpoint: str, data=None, headers=None
):
    """Function to perform the request to the ModelIndex server

    Args:
        method (str): "GET" or "POST"
        endpoint (str): The last part of the url (without the leading "/")
        data (str): defaults to None but can contain the data to send to the endpoint
        headers (str): default to None but can contain the headers og the request
    Returns:
        JSON: The result if successfull
    """
    timeout = (3, 27)
    if method == "GET":
        result = requests.get(rest_url + endpoint, timeout=timeout)
    elif method == "POST":
        result = requests.post(
            rest_url + endpoint, data=data, headers=headers, timeout=timeout
        )
    else:
        raise Exception("Method not supported")
    result.raise_for_status()
    return result.json()


def normalize_as_dataframe(content: str):
    """Normalizes the input JSON and turns it into a Pandas
    Dataframe with the following colums:
    - Id
    - Type
    - Name
    - Props
    - Vars

    Args:
        content (str): the JSON to convert
    Returns:
        pandas.DataFrame: a normalized dataframe
    """
    if content is None:
        return None

    df = pd.DataFrame(content)

    # Remove "Subtype"
    if "Subtype" in df.columns:
        df.drop("Subtype", inplace=True, axis=1)

    # Remove "ObjectId"
    if "ObjectId" in df.columns:
        df.drop("ObjectId", inplace=True, axis=1)

    # Remove "ObjectId"
    if "ObjectName" in df.columns:
        df.drop("ObjectName", inplace=True, axis=1)

    # Check if the content is from get_objects_of_type
    if "DisplayName" in df.columns:
        df.rename(columns={"DisplayName": "Name"}, inplace=True)

    # Check if the content is from object-descendants
    if "DescendantId" in df.columns:
        df.rename(
            columns={
                "DescendantId": "Id",
                "DescendantName": "Name",
                "DescendantType": "Type",
            },
            inplace=True,
        )

    # Check if the content is from object-ancestors
    if "AncestorId" in df.columns:
        df.rename(
            columns={
                "AncestorId": "Id",
                "AncestorName": "Name",
                "AncestorType": "Type",
            },
            inplace=True,
        )

    return df


def get_ids_from_dataframe(obj_dataframe: pd.DataFrame) -> list:
    """Extracts data from "Id" column in a DataFrame or an empty list if none.

    Args:
        obj_dataframe (pd.DataFrame): DataFrame with a column called "Id"

    Returns:
        list: a list with ids (empty if None)
    """

    if "Id" in obj_dataframe.columns:
        return obj_dataframe["Id"].to_list()

    return []
