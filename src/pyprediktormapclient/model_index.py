import json
import logging
from typing import List
from pydantic import AnyUrl, validate_call
from pyprediktormapclient.shared import request_from_api

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class ModelIndex:
    """Helper functions to access the ModelIndex API server

    Args:
        url (str): The URL of the ModelIndex server with the trailing slash

    Todo:
        * Validate combination of url and endpoint
    """

    @validate_call
    def __init__(self, url: AnyUrl):
        self.url = url
        self.object_types = self.get_object_types()

    def get_namespace_array(self) -> str:
        """Get the namespace array

        Returns:
            str: the JSON returned from the server
        """
        content = request_from_api(self.url, "GET", "query/namespace-array")

        return content

    def get_object_types(self) -> str:
        content = request_from_api(self.url, "GET", "query/object-types")

        return content

    @validate_call
    def get_object_type_id_from_name(self, type_name: str) -> str:
        """Get object type id from type name

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

    @validate_call
    def get_objects_of_type(self, type_name: str) -> str:
        """Function to get all the types of an object

        Args:
            type_name (str): type name

        Returns:
            A json-formatted string with the objects (or None if the type is not found)
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        if object_type_id is None:
            return None

        body = json.dumps({"typeId": object_type_id})
        content = request_from_api(self.url, "POST", "query/objects-of-type", body)

        return content

    @validate_call
    def get_object_descendants(
        self,
        type_name: str,
        ids: List,
        domain: str,
    ) -> str:
        """A function to get object descendants

        Args:
            type_name (str): type_name of a descendant
            ids (list): a list of ids you want the descendants for
            domain (str): PV_Assets or PV_Serves

        Returns:
            A json-formatted string with descendats data of selected object (or None if the type is not found)
        """
        id = self.get_object_type_id_from_name(type_name)
        body = json.dumps(
            {
                "typeId": id,
                "objectIds": ids,
                "domain": domain,
            }
        )
        content = request_from_api(self.url, "POST", "query/object-descendants", body)

        return content

    @validate_call
    def get_object_ancestors(
        self,
        type_name: str,
        ids: List,
        domain: str,
    ) -> str:
        """Function to get object ancestors

        Args:
            type_name (str): the ancestor parent type
            ids (list): a list of ids you want the ancestors for
            domain (str): Either PV_Assets or PV_Serves

        Returns:
            A json-formatted string with ancestors data of selected object (or None if the type is not found)
        """
        id = self.get_object_type_id_from_name(type_name)
        body = json.dumps(
            {
                "typeId": id,
                "objectIds": ids,
                "domain": domain,
            }
        )
        content = request_from_api(self.url, "POST", "query/object-ancestors", body)

        return content
