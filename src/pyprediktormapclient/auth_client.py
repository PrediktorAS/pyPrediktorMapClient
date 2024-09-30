import datetime
import json
import re
from typing import Optional

import requests
from dateutil.parser import ParserError, parse
from dateutil.tz import tzutc
from pydantic import AnyUrl, BaseModel, ConfigDict, field_validator

from pyprediktormapclient.shared import request_from_api


class Ory_Login_Structure(BaseModel):
    method: str
    identifier: str
    password: str


class Token(BaseModel):
    session_token: str
    expires_at: Optional[datetime.datetime] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("expires_at", mode="before")
    def remove_nanoseconds(cls, v):
        if v is None:
            return v
        if isinstance(v, datetime.datetime):
            return v
        match = re.match(
            r"(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d).\d+(\S+)", v
        )
        if match:
            return datetime.datetime.strptime(
                match.group(0)[:-4] + match.group(7), "%Y-%m-%dT%H:%M:%S.%f%z"
            )
        return v


class AUTH_CLIENT:
    """Helper functions to authenticate with Ory.

    Args:
        rest_url (str): The complete url of the OPC UA Values REST API. E.g. "http://127.0.0.1:13371/"
        opcua_url (str): The complete url of the OPC UA Server that is passed on to the REST server. E.g. "opc.tcp://127.0.0.1:4872"
        namespaces (list): An optional but recommended ordered list of namespaces so that IDs match

    Returns:
        Object
    """

    def __init__(self, rest_url: AnyUrl, username: str, password: str):
        """Class initializer.

        Args:
            rest_url (str): The complete url of the Ory server. E.g. "http://127.0.0.1:9099/"
            username (str): The username of the user
            password (str): The password of the user
        Returns:
            Object: The initialized class object
        """
        self.rest_url = rest_url
        self.username = username
        self.password = password
        self.id = None
        self.token = None
        self.headers = {"Content-Type": "application/json"}
        self.session = requests.Session()

    def get_login_id(self) -> None:
        """Request login token from Ory."""
        content = request_from_api(
            rest_url=self.rest_url,
            method="GET",
            endpoint="self-service/login/api",
            headers=self.headers,
            extended_timeout=True,
        )
        if "error" in content:
            # Handle the error appropriately
            raise RuntimeError(content["error"])

        if content.get("Success") is False or not isinstance(
            content.get("id"), str
        ):
            error_message = content.get(
                "ErrorMessage", "Unknown error occurred during login."
            )
            raise RuntimeError(error_message)

        self.id = content.get("id")

    def get_login_token(self) -> None:
        """Request login token from Ory."""
        params = {"flow": self.id}
        body = Ory_Login_Structure(
            method="password", identifier=self.username, password=self.password
        ).model_dump()
        content = request_from_api(
            rest_url=self.rest_url,
            method="POST",
            endpoint="self-service/login",
            data=json.dumps(body),
            params=params,
            headers=self.headers,
            extended_timeout=True,
        )

        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))

        # Return if no content from server
        if not isinstance(content.get("session_token"), str):
            raise RuntimeError(content.get("ErrorMessage"))

        session_token = content.get("session_token")
        expires_at = None

        # Check if token has expiry date, save it if it does
        expires_at_str = content.get("session", {}).get("expires_at")
        if isinstance(expires_at_str, str):
            try:
                expires_at = parse(expires_at_str).replace(tzinfo=tzutc())
            except ParserError:
                expires_at = None

        self.token = Token(session_token=session_token, expires_at=expires_at)

    def check_if_token_has_expired(self) -> bool:
        """Check if token has expired."""
        if self.token is None or self.token.expires_at is None:
            return True

        return (
            datetime.datetime.now(datetime.timezone.utc)
            > self.token.expires_at
        )

    def request_new_ory_token(self) -> None:
        """Request Ory token."""
        self.get_login_id()
        self.get_login_token()
