import json
import pandas as pd
from pydantic import BaseModel, HttpUrl
from pyprediktormapclient.shared import request_from_api, normalize_as_dataframe


class RESTUrls(BaseModel):
    rest_url: HttpUrl


class ModelIndex:
    """Data structure from the model index API server"""

    def __init__(self, url: str):
        RESTUrls(rest_url=url)
        self.url = url
        self.object_types = self.get_object_types(return_format="json")

    def get_namespace_array(self, return_format="json") -> str:
        content = request_from_api(self.url, "GET", "query/namespace-array")
        if return_format == "dataframe":
            return normalize_as_dataframe(content)
        return content

    def get_object_types(self, return_format="json") -> str:
        content = request_from_api(self.url, "GET", "query/object-types")
        if return_format == "dataframe":
            return normalize_as_dataframe(content)

        return content

    def get_object_type_id_from_name(self, type_name: str) -> str:
        """Function to get object type id from type name

        Args:
            type_name (str): type name

        Returns:
            str: the type id that corresponds with the id (None if not found)
        """
        try:
            obj_type = next(
                item for item in self.object_types if item["BrowseName"] == type_name
            )
        except StopIteration:
            obj_type = {}
        object_type_id = obj_type.get("Id")  # Returns None if the ID is not present

        return object_type_id

    def get_objects_of_type(self, type_name: str, return_format="json") -> str:
        """Function to get all the types of an object

        Args:
            type_name (str): type name

        Returns:
            pd.DataFrame or JSON: a Dataframe or JSON with the objects (or None if the type is not found)
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        if object_type_id is None:
            return None

        body = json.dumps({"typeId": object_type_id})
        content = request_from_api(self.url, "POST", "query/objects-of-type", body)
        if return_format == "dataframe":
            return normalize_as_dataframe(content)

        return content

    def get_object_descendants(
        self,
        type_name: str,
        ids: list,
        domain: str,
        return_format="json",
    ) -> str:
        """A function to get object descendants

        Args:
            type_name (str): type_name of a descendant
            ids (list): a list of ids you want the descendants for
            domain (str): PV_Assets or PV_Serves

        Returns:
            pd.DataFrame or JSON: descendats data of selected object (or None if the type is not found)
        """
        id = self.get_object_type_id_from_name(type_name)
        if id is None:
            return None

        if ids is None:
            return None

        body = json.dumps(
            {
                "typeId": id,
                "objectIds": ids,
                "domain": domain,
            }
        )
        content = request_from_api(self.url, "POST", "query/object-descendants", body)
        if return_format == "dataframe":
            return normalize_as_dataframe(content)

        return content

    def get_object_ancestors(
        self,
        type_name: str,
        ids: list,
        domain: str,
        return_format="json",
    ) -> str:
        """Function to get object ancestors

        Args:
            type_name (str): the ancestor parent type
            ids (list): a list of ids you want the ancestors for
            domain (str): Either PV_Assets or PV_Serves

        Returns:
            pd.DataFrame or JSON: ancestors data of selected object (or None if the type is not found)
        """
        id = self.get_object_type_id_from_name(type_name)
        if id is None:
            return None

        if ids is None:
            return None

        body = json.dumps(
            {
                "typeId": id,
                "objectIds": ids,
                "domain": domain,
            }
        )
        content = request_from_api(self.url, "POST", "query/object-ancestors", body)
        if return_format == "dataframe":
            return normalize_as_dataframe(content)

        return content
