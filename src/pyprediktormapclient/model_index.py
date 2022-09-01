import requests
import json
import pandas as pd
from pydantic import BaseModel, HttpUrl, AnyUrl
import logging

logger = logging.getLogger()

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
        if content is None:
            return None
        return pd.DataFrame(content)

    def request(self, method: str, endpoint: str, data=None) -> json:       
        if method == 'GET':
            result = requests.get(self.url + endpoint)
        elif method == 'POST':
            result = requests.post(self.url + endpoint, data=data)
        else:
            raise Exception('Method not supported')
        if result.status_code == 200:
            return result.json()
        else:
            return None

    def get_namespace_array(self, return_format="dataframe") -> json:
        content = self.request('GET', 'query/namespace-array')
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_types(self, return_format="dataframe") -> json:
        content = self.request('GET', 'query/object-types')
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_type_id_from_name(self, type_name: str) -> str:
        """Function to get object type id from type name
        """
        try:
            obj_type =  next(item for item in self.object_types if item["BrowseName"] == type_name)
        except StopIteration:
            obj_type = {}
        object_type_id = obj_type.get("Id")
        return object_type_id

    def get_objects_of_type(self, type_name: str, return_format="dataframe"):
        """Function to get all the types of an object

        Args:
            type_name (str): type name 
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        body = json.dumps({"typeId": object_type_id})
        content = self.request('POST', 'query/objects-of-type', body)
        if return_format == "dataframe":
            return self.as_dataframe(content)
        return content

    def get_object_descendants(self, type_name: str, obj_dataframe: pd.DataFrame, domain: str, return_format="dataframe") -> json:
        """A function to get object descendants

        Args:
            type_name (str): type_name of a descendant
            obj_dataframe (pd.DataFrame): dataframe of object ids
            domain (str): PV_Assets or PV_Serves

        Returns:
            pd.DataFrame: descendats data of selected object
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

    def get_object_ancestors(self, type_name: str, obj_dataframe: pd.DataFrame, domain: str, return_format="dataframe") -> json:
        """Function to get object ancestors

        Args:
            type_name (str): type_name of a parent type
            obj_dataframe (pd.DataFrame): dataframe of object ids
            domain (str): Either PV_Assets or PV_Serves

        Returns:
            pd.DataFrame: ancestors data of selected object
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

