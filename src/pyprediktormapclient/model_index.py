import json
import logging
from datetime import date, datetime
from typing import List

import requests
from pydantic import AnyUrl
from pydantic_core import Url

from pyprediktormapclient.shared import request_from_api

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class ModelIndex:
    """Helper functions to access the ModelIndex API server.

    Args:
        url (str): The URL of the ModelIndex server with the trailing slash

    Todo:
        * Validate combination of url and endpoint
    """

    class Config:
        arbitrary_types_allowed = True

    def __init__(
        self,
        url: AnyUrl,
        auth_client: object = None,
        session: requests.Session = None,
    ):
        self.url = url
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.auth_client = auth_client
        self.session = session

        if self.auth_client is not None:
            if self.auth_client.token is not None:
                self.headers["Authorization"] = (
                    f"Bearer {self.auth_client.token.session_token}"
                )
            if hasattr(self.auth_client, "session_token"):
                self.headers["Cookie"] = (
                    f"ory_kratos_session={self.auth_client.session_token}"
                )

        self.body = {"Connection": {"Url": self.url, "AuthenticationType": 1}}
        self.object_types = self.get_object_types()

    def json_serial(self, obj):
        """JSON serializer for objects not serializable by default json
        code."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Url):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    def check_auth_client(self, content):
        if content.get("error", {}).get("code") == 404:
            self.auth_client.request_new_ory_token()
            self.headers["Authorization"] = (
                f"Bearer {self.auth_client.token.session_token}"
            )
        else:
            raise RuntimeError(content.get("ErrorMessage"))

    def get_namespace_array(self) -> str:
        """Get the namespace array.

        Returns:
            str: the JSON returned from the server
        """
        content = request_from_api(
            self.url,
            "GET",
            "query/namespace-array",
            headers=self.headers,
            session=self.session,
        )
        return content

    def get_object_types(self) -> str:
        content = request_from_api(
            self.url,
            "GET",
            "query/object-types",
            headers=self.headers,
            session=self.session,
        )
        return content

    def get_object_type_id_from_name(self, type_name: str) -> str:
        """Get object type id from type name.

        Args:
            type_name (str): type name

        Returns:
            str: the type id that corresponds with the id (None if not found)
        """
        try:
            obj_type = next(
                item
                for item in self.object_types
                if item["BrowseName"] == type_name
            )
        except StopIteration:
            obj_type = {}
        object_type_id = obj_type.get("Id")
        return object_type_id

    def get_objects_of_type(self, type_name: str) -> str:
        """Function to get all the types of an object.

        Args:
            type_name (str): type name

        Returns:
            A json-formatted string with the objects (or None if the type is not found)
        """
        object_type_id = self.get_object_type_id_from_name(type_name)
        if object_type_id is None:
            return None

        body = json.dumps({"typeId": object_type_id})
        content = request_from_api(
            self.url,
            "POST",
            "query/objects-of-type",
            body,
            headers=self.headers,
            session=self.session,
        )
        return content

    def get_object_descendants(
        self,
        type_name: str,
        ids: List,
        domain: str,
    ) -> str:
        """A function to get object descendants.

        Args:
            type_name (str): type_name of a descendant
            ids (list): a list of ids you want the descendants for
            domain (str): PV_Assets or PV_Serves

        Returns:
            A json-formatted string with descendats data of selected object (or None if the type is not found)
        """
        if type_name is None or not ids:
            raise ValueError("type_name and ids cannot be None or empty")

        id = self.get_object_type_id_from_name(type_name)
        body = json.dumps(
            {
                "typeId": id,
                "objectIds": ids,
                "domain": domain,
            }
        )
        content = request_from_api(
            self.url,
            "POST",
            "query/object-descendants",
            body,
            headers=self.headers,
            session=self.session,
        )
        return content

    def get_object_ancestors(
        self,
        type_name: str,
        ids: List,
        domain: str,
    ) -> str:
        """Function to get object ancestors.

        Args:
            type_name (str): the ancestor parent type
            ids (list): a list of ids you want the ancestors for
            domain (str): Either PV_Assets or PV_Serves

        Returns:
            A json-formatted string with ancestors data of selected object (or None if the type is not found)
        """
        if type_name is None or not ids:
            raise ValueError("type_name and ids cannot be None or empty")

        id = self.get_object_type_id_from_name(type_name)
        body = json.dumps(
            {
                "typeId": id,
                "objectIds": ids,
                "domain": domain,
            }
        )
        content = request_from_api(
            self.url,
            "POST",
            "query/object-ancestors",
            body,
            headers=self.headers,
            session=self.session,
        )
        return content
