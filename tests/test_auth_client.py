import unittest
from unittest import mock
import pytest
from pydantic import ValidationError
from copy import deepcopy
import datetime

from pyprediktormapclient.auth_client import AUTH_CLIENT, Token

URL = "http://someserver.somedomain.com/v1/"
username = "some@user.com"
password = "somepassword"
auth_id = "0b518533-fb09-4bb7-a51f-166d3453685e"
auth_session_id = "qlZULxcaNc6xVdXQfqPxwix5v3tuCLaO"
auth_expires_at = "2022-12-04T07:31:28.767407252Z"
auth_expires_at_2hrs_ago = "2022-12-04T05:31:28.767407252Z"

successful_self_service_login_token_mocked_response = {
                            "Success": True,
                            "ErrorMessage": "",
                            "ErrorCode": 0,
                            "session_token": "qlZULxcaNc6xVdXQfqPxwix5v3tuCLaO",
                            "session": {
                                "id": "b26e9032-c34b-4f14-a912-c055b12f02da",
                                "active": True,
                                "expires_at": "2022-12-04T07:31:28.767407252Z",
                                "authenticated_at": "2022-12-01T07:31:28.767407252Z",
                                "authenticator_assurance_level": "aal1",
                                "authentication_methods": [
                                    {
                                        "method": "password",
                                        "aal": "aal1",
                                        "completed_at": "2022-12-01T07:31:28.767403652Z"
                                    }
                                ],
                                "issued_at": "2022-12-01T07:31:28.767407252Z",
                                "identity": {
                                    "id": "24f9466f-4e81-42d8-8a40-1f46f2203c19",
                                    "schema_id": "preset://email",
                                    "schema_url": "https://authauth.blabla.io/schemas/cHJlc2V0Oi8vZW1haWw",
                                    "state": "active",
                                    "state_changed_at": "2022-08-15T12:43:28.623721Z",
                                    "traits": {
                                        "email": "api@prediktor.com"
                                    },
                                    "verifiable_addresses": [
                                        {
                                            "id": "c083c747-eee9-4ee0-a7b9-96aa067bcea6",
                                            "value": "api@prediktor.com",
                                            "verified": False,
                                            "via": "email",
                                            "status": "sent",
                                            "created_at": "2022-08-15T12:43:28.631351Z",
                                            "updated_at": "2022-08-15T12:43:28.631351Z"
                                        }
                                    ],
                                    "recovery_addresses": [
                                        {
                                            "id": "f907259d-52d9-420b-985a-004b6e745bd0",
                                            "value": "api@prediktor.com",
                                            "via": "email",
                                            "created_at": "2022-08-15T12:43:28.637771Z",
                                            "updated_at": "2022-08-15T12:43:28.637771Z"
                                        }
                                    ],
                                    "metadata_public": None,
                                    "created_at": "2022-08-15T12:43:28.626258Z",
                                    "updated_at": "2022-08-15T12:43:28.626258Z"
                                },
                                "devices": [
                                    {
                                        "id": "5f805841-d4fc-461f-ad1b-87ca4166355d",
                                        "ip_address": "81.166.54.38",
                                        "user_agent": "Go-http-client/1.1",
                                        "location": "Fredrikstad, NO"
                                    }
                                ]
                            }
                        }

# TODO handle old id to get new token, use use_flow_id, investigate if this works
expired_self_service_logon_response = {
                            "error": {
                                "id": "self_service_flow_expired",
                                "code": 410,
                                "status": "Gone",
                                "reason": "The self-service flow expired 904.65 minutes ago, initialize a new one.",
                                "message": "self-service flow expired"
                            },
                            "expired_at": "2022-11-24T15:37:59.690422Z",
                            "since": 54279099192303,
                            "use_flow_id": "2b132a0d-f3a5-4eff-8a1b-f785228168a9"
                        }



successfull_self_service_login_response ={
                                "Success": True,
                                "ErrorMessage": "",
                                "ErrorCode": 0,
                                "id": "0b518533-fb09-4bb7-a51f-166d3453685e",
                                "oauth2_login_challenge": None,
                                "type": "api",
                                "expires_at": "2022-11-24T15:37:59.690422428Z",
                                "issued_at": "2022-11-24T14:37:59.690422428Z",
                                "request_url": "https://authauth.blabla.io/self-service/login/api",
                                "ui": {
                                    "action": "https://authauth.blabla.io/self-service/login?flow=0b518533-fb09-4bb7-a51f-166d3453685e",
                                    "method": "POST",
                                    "nodes": [
                                        {
                                            "type": "input",
                                            "group": "default",
                                            "attributes": {
                                                "name": "csrf_token",
                                                "type": "hidden",
                                                "value": "",
                                                "required": True,
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {}
                                        },
                                        {
                                            "type": "input",
                                            "group": "default",
                                            "attributes": {
                                                "name": "identifier",
                                                "type": "text",
                                                "value": "",
                                                "required": True,
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {
                                                "label": {
                                                    "id": 1070004,
                                                    "text": "ID",
                                                    "type": "info"
                                                }
                                            }
                                        },
                                        {
                                            "type": "input",
                                            "group": "password",
                                            "attributes": {
                                                "name": "password",
                                                "type": "password",
                                                "required": True,
                                                "autocomplete": "current-password",
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {
                                                "label": {
                                                    "id": 1070001,
                                                    "text": "Password",
                                                    "type": "info"
                                                }
                                            }
                                        },
                                        {
                                            "type": "input",
                                            "group": "password",
                                            "attributes": {
                                                "name": "method",
                                                "type": "submit",
                                                "value": "password",
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {
                                                "label": {
                                                    "id": 1010001,
                                                    "text": "Sign in",
                                                    "type": "info",
                                                    "context": {}
                                                }
                                            }
                                        }
                                    ]
                                },
                                "created_at": "2022-11-24T14:37:59.770365Z",
                                "updated_at": "2022-11-24T14:37:59.770365Z",
                                "refresh": False,
                                "requested_aal": "aal1"
                            }
empty_self_service_login_response = {
                                "Success": True,
                                "ErrorMessage": "",
                                "ErrorCode": 0,
                                "oauth2_login_challenge": None,
                                "type": "api",
                                "expires_at": "2022-11-24T15:37:59.690422428Z",
                                "issued_at": "2022-11-24T14:37:59.690422428Z",
                                "request_url": "https://authauth.blabla.io/self-service/login/api",
                                "ui": {
                                    "method": "POST",
                                    "nodes": [
                                        {
                                            "type": "input",
                                            "group": "default",
                                            "attributes": {
                                                "name": "csrf_token",
                                                "type": "hidden",
                                                "value": "",
                                                "required": True,
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {}
                                        },
                                        {
                                            "type": "input",
                                            "group": "default",
                                            "attributes": {
                                                "name": "identifier",
                                                "type": "text",
                                                "value": "",
                                                "required": True,
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {
                                                "label": {
                                                    "id": 1070004,
                                                    "text": "ID",
                                                    "type": "info"
                                                }
                                            }
                                        },
                                        {
                                            "type": "input",
                                            "group": "password",
                                            "attributes": {
                                                "name": "password",
                                                "type": "password",
                                                "required": True,
                                                "autocomplete": "current-password",
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {
                                                "label": {
                                                    "id": 1070001,
                                                    "text": "Password",
                                                    "type": "info"
                                                }
                                            }
                                        },
                                        {
                                            "type": "input",
                                            "group": "password",
                                            "attributes": {
                                                "name": "method",
                                                "type": "submit",
                                                "value": "password",
                                                "disabled": False,
                                                "node_type": "input"
                                            },
                                            "messages": [],
                                            "meta": {
                                                "label": {
                                                    "id": 1010001,
                                                    "text": "Sign in",
                                                    "type": "info",
                                                    "context": {}
                                                }
                                            }
                                        }
                                    ]
                                },
                                "created_at": "2022-11-24T14:37:59.770365Z",
                                "updated_at": "2022-11-24T14:37:59.770365Z",
                                "refresh": False,
                                "requested_aal": "aal1"
                            }

class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code
        self.raise_for_status = mock.Mock(return_value=False)

    def json(self):
        return self.json_data


# This method will be used by the mock to replace requests
def successful_self_service_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login/api":
        return MockResponse(successfull_self_service_login_response, 200)
    return MockResponse(None, 404)


def empty_self_service_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login/api":
        return MockResponse(empty_self_service_login_response, 200)
    return MockResponse(None, 404)

def unsuccessful_self_service_login_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login/api":
        # Set Success to False
        unsuc = deepcopy(successfull_self_service_login_response)
        unsuc["Success"] = False
        return MockResponse(unsuc, 200)

    return MockResponse(None, 404)

def wrong_id_self_service_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login/api":
        unsuc = deepcopy(successfull_self_service_login_response)
        unsuc["id"] = 33
        return MockResponse(unsuc, 200)

    return MockResponse(None, 404)

def successful_self_service_login_token_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login":
        return MockResponse(successful_self_service_login_token_mocked_response, 200)

    return MockResponse(None, 404)

def unsuccessful_self_service_login_token_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login":
        unsuc = deepcopy(successful_self_service_login_token_mocked_response)
        unsuc["Success"] = False
        return MockResponse(unsuc, 200)

    return MockResponse(None, 404)

def empty_self_service_login_token_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login":
        return MockResponse(empty_self_service_login_response, 200)

    return MockResponse(None, 404)




def expired_self_service_login_mocked_requests(*args, **kwargs):
    if args[0] == f"{URL}self-service/login":
        return MockResponse(expired_self_service_login_mocked_requests, 200)

    return MockResponse(None, 404)



# Our test case class
class OPCUATestCase(unittest.TestCase):

    def test_malformed_rest_url(self):
        with pytest.raises(ValidationError):
            auth_client = AUTH_CLIENT(rest_url="htio/dsadsadsa", username=username, password=password)

    @mock.patch("requests.get", side_effect=successful_self_service_mocked_requests)
    def test_get_self_service_login_id_successful(self, mock_get):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        auth_client.get_login_id()
        assert auth_client.id == auth_id

    @mock.patch("requests.get", side_effect=unsuccessful_self_service_login_mocked_requests)
    def test_get_self_service_login_id_successful(self, mock_get):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        with pytest.raises(RuntimeError):
            auth_client.get_login_id()

    @mock.patch("requests.get", side_effect=empty_self_service_mocked_requests)
    def test_get_self_service_login_id_empty(self, mock_get):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        with pytest.raises(RuntimeError):
            auth_client.get_login_id()

    @mock.patch("requests.get", side_effect=wrong_id_self_service_mocked_requests)
    def test_get_self_service_login_id_wrong_id(self, mock_get):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        with pytest.raises(RuntimeError):
            auth_client.get_login_id()

    @mock.patch("requests.post", side_effect=empty_self_service_login_token_mocked_requests)
    def test_get_self_service_login_token_empty(self, mock_get):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        auth_client.id = auth_id
        with pytest.raises(RuntimeError):
            auth_client.get_login_token()

    @mock.patch("requests.post", side_effect=successful_self_service_login_token_mocked_requests)
    def test_get_self_service_login_token_successful(self, mock_get):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        auth_client.id = auth_id
        auth_client.get_login_token()
        test_token = Token(access_token=auth_session_id, expires_at=auth_expires_at)
        assert auth_client.token.access_token == test_token.access_token
        assert auth_client.token.expires_at == test_token.expires_at

    @mock.patch("requests.post", side_effect=unsuccessful_self_service_login_token_mocked_requests)
    def test_get_self_service_login_token_unsuccessful(self, mock_get):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        auth_client.id = auth_id
        with pytest.raises(RuntimeError):
            auth_client.get_login_token()


    def test_get_self_service_token_expired(self):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        auth_client.token = Token(access_token=auth_session_id, expires_at=auth_expires_at_2hrs_ago)
        auth_client.token.expires_at = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
        token_expired = auth_client.check_if_token_has_expired()
        assert token_expired == True

    def test_get_self_service_token_expired_none(self):
        auth_client = AUTH_CLIENT(rest_url=URL, username=username, password=password)
        auth_client.token = Token(access_token=auth_session_id)
        token_expired = auth_client.check_if_token_has_expired()
        assert token_expired == True

if __name__ == "__main__":
    unittest.main()
