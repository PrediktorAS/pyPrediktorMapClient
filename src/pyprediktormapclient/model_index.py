import json
import pandas as pd
from pydantic import BaseModel, HttpUrl
from pyprediktormapclient.shared import request_from_api

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

    def get_namespace_array(self, return_format="dataframe") -> str:
        content = request_from_api(self.url, 'GET', 'query/namespace-array')
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_types(self, return_format="dataframe") -> str:
        content = request_from_api(self.url, 'GET', 'query/object-types')
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

    def get_object_ids_from_dataframe(self, obj_dataframe: pd.DataFrame) -> list:
        """Extracts data from one of the three columns in the supplied
        Pandas DataFrame as list: "Id", "DescendantId", "AncestorId".

        Args:
            obj_dataframe (pd.DataFrame): DataFrame with a column called "Id", "DescendantId" or "AncestorId" 

        Returns:
            list: a list with ids (empty if None)
        """
        try:
            id_column = [x for x in obj_dataframe if x in ['Id','DescendantId', 'AncestorId']][0]
        except IndexError:
            return []

        return obj_dataframe[id_column].to_list()


    def get_objects_of_type(self, type_name: str, return_format="dataframe") -> str:
        """Function to get all the types of an object

        Args:
            type_name (str): type name

        Returns:
            pd.DataFrame or JSON: a Dataframe or JSON with the objects
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        body = json.dumps({"typeId": object_type_id})
        content = request_from_api(self.url, 'POST', 'query/objects-of-type', body)
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
        body = json.dumps({
            "typeId": self.get_object_type_id_from_name(type_name),
            "objectIds": self.get_object_ids_from_dataframe(obj_dataframe),
            "domain": domain
            })
        content = request_from_api(self.url, 'POST', 'query/object-descendants', body)
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
        body = json.dumps({
            "typeId": self.get_object_type_id_from_name(type_name),
            "objectIds": self.get_object_ids_from_dataframe(obj_dataframe),
            "domain": domain
            })
        content = request_from_api(self.url, 'POST', 'query/object-ancestors', body)
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

