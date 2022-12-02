from pydantic import BaseModel, HttpUrl, validate_arguments
from pyprediktormapclient.shared import request_from_api
import datetime
import json

class Ory_Login_Structure(BaseModel):
    method: str
    identifier: str
    password: str

class Token(BaseModel):
    access_token: str
    expires_at: datetime.datetime = None
    expired: bool = None

class AUTH_CLIENT:
    """Helper functions to authenticate with Ory

    Args:
        rest_url (str): The complete url of the OPC UA Values REST API. E.g. "http://127.0.0.1:13371/"
        opcua_url (str): The complete url of the OPC UA Server that is passed on to the REST server. E.g. "opc.tcp://127.0.0.1:4872"
        namespaces (list): An optional but recommended ordered list of namespaces so that IDs match

    Returns:
        Object

    """
    @validate_arguments
    def __init__(self, rest_url: HttpUrl, username: str, password: str):
        """Class initializer

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

    @validate_arguments
    def get_login_id(self) -> None:
        """Request login token from Ory
        """
        content = request_from_api(
            rest_url=self.rest_url,
            method="GET",
            endpoint="self-service/login/api",
            headers=self.headers,
            extended_timeout=True,
        )
        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))
        
        # Return if no content for id from server, then no further login is possible
        if not isinstance(content.get("id"), str):
            raise RuntimeError(content.get("ErrorMessage"))

        self.id = content.get("id")

    @validate_arguments
    def get_login_token(self) -> None:
        """Request login token from Ory
        """
        params = {"flow": self.id}
        body = (Ory_Login_Structure(method="password", identifier=self.username, password=self.password).dict())
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
        self.token = Token(access_token=content.get("session_token"))

        # Check if token has expiry date, save it if it does
        if isinstance(content.get("session").get("expires_at"), str):
            # String returned from ory has to many chars in microsec. Remove them
            try:
                self.token = Token(access_token=self.token.access_token, expires_at=content.get("session").get("expires_at"), expired=None)
            except Exception:
                # If string returned from Ory cant be parsed, still should be possible to use Ory,
                #  might be a setting in Ory to not return expiry date
                self.token = Token(access_token=self.token.access_token, expires_at=None, expired=None)

    def check_if_token_has_expired(self) -> bool:
        """Check if token has expired
        """
        if self.token.expires_at is None:
            return False
        return datetime.datetime.utcnow() > self.token.expires_at

    def request_new_ory_token(self) -> None:
        """Request Ory token
        """
        self.get_login_id()
        self.get_login_token()