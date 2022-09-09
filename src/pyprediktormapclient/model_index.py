import requests
import json
import pandas as pd
from pydantic import BaseModel, HttpUrl

class RESTUrls(BaseModel):
    rest_url: HttpUrl

class ModelIndex:
    """Data structure from the model index API server
    """
    def __init__(self, url: str):
        RESTUrls(rest_url=url)
        self.url = url
        self.object_types = self.get_object_types(return_format="json")

    def as_dataframe(self, content) -> pd.DataFrame:
        """Function to convert a json string to Pandas DataFrame

        Args:
            content (str): the json string 
        """
        if content is None:
            return None
        return pd.DataFrame(content)

    def request(self, method: str, endpoint: str, data=None, headers=None) -> str:
        """Function to perform the request to the ModelIndex server

        Args:
            method (str): "GET" or "POST"
            endpoint (str): The last part of the url (without the leading "/") 
            data (str): defaults to None but can contain the data to send to the endpoint
            headers (str): default to None but can contain the headers og the request
        Returns:
            JSON: The result if successfull
        """
        if method == 'GET':
            result = requests.get(self.rest_url + endpoint, timeout=(3, 27))
        elif method == 'POST':
            result = requests.post(self.rest_url + endpoint, data=data, headers=headers, timeout=(3, 27))
        else:
            raise Exception('Method not supported')
        result.raise_for_status()
        return result.json()

    def get_namespace_array(self, return_format="dataframe") -> str:
        content = self.request('GET', 'query/namespace-array')
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_types(self, return_format="dataframe") -> str:
        content = self.request('GET', 'query/object-types')
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_type_id_from_name(self, type_name: str) -> str:
        """Function to get object type id from type name

        Args:
            type_name (str): type name

        Returns:
            str: the type id that corresponds with the id
        """
        try:
            obj_type =  next(item for item in self.object_types if item["BrowseName"] == type_name)
        except StopIteration:
            obj_type = {}
        object_type_id = obj_type.get("Id")
        return object_type_id

    def get_objects_of_type(self, type_name: str, return_format="dataframe") -> str:
        """Function to get all the types of an object

        Args:
            type_name (str): type name

        Returns:
            pd.DataFrame or JSON: a Dataframe or JSON with the objects
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        body = json.dumps({"typeId": object_type_id})
        content = self.request('POST', 'query/objects-of-type', body)
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_descendants(self, type_name: str, obj_dataframe: pd.DataFrame, domain: str, return_format="dataframe") -> str:
        """A function to get object descendants

        Args:
            type_name (str): type_name of a descendant
            obj_dataframe (pd.DataFrame): dataframe of object ids
            domain (str): PV_Assets or PV_Serves

        Returns:
            pd.DataFrame or JSON: descendats data of selected object
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        id_column = [x for x in obj_dataframe if x in ['Id','DescendantId', 'AncestorId']][0]
        object_Ids = obj_dataframe[id_column].to_list()
        body = json.dumps({
            "typeId": object_type_id,
            "objectIds": object_Ids,
            "domain": domain
            })
        content = self.request('POST', 'query/object-descendants', body)
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_ancestors(self, type_name: str, obj_dataframe: pd.DataFrame, domain: str, return_format="dataframe") -> str:
        """Function to get object ancestors

        Args:
            type_name (str): type_name of a parent type
            obj_dataframe (pd.DataFrame): dataframe of object ids
            domain (str): Either PV_Assets or PV_Serves

        Returns:
            pd.DataFrame or JSON: ancestors data of selected object
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        id_column = [x for x in obj_dataframe if x in ['Id','AncestorId', 'DescendantId']][0]
        object_Ids = obj_dataframe[id_column].to_list()
        body = json.dumps({
            "typeId": object_type_id,
            "objectIds": object_Ids,
            "domain": domain
            })
        content = self.request('POST', 'query/object-ancestors', body)
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

