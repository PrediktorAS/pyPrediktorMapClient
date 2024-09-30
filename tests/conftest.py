import random
import string
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiohttp import ClientResponseError

from pyprediktormapclient.auth_client import AUTH_CLIENT
from pyprediktormapclient.dwh.db import Db
from pyprediktormapclient.dwh.dwh import DWH
from pyprediktormapclient.opc_ua import OPC_UA

URL = "http://someserver.somedomain.com/v1/"
username = "some@user.com"
password = "somepassword"
OPC_URL = "opc.tcp://nosuchserver.nosuchdomain.com"


def grs():
    """Generate a random string."""
    return "".join(
        random.choices(string.ascii_uppercase + string.digits, k=10)
    )


@pytest.fixture
def opc():
    opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
    return opc


@pytest.fixture
def auth_client():
    return AUTH_CLIENT(rest_url=URL, username=username, password=password)


@pytest.fixture
def mock_pyodbc_connect(monkeypatch):
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr(
        "pyodbc.connect", lambda *args, **kwargs: mock_connection
    )
    return mock_cursor


@pytest.fixture
def mock_pyodbc_drivers(monkeypatch):
    monkeypatch.setattr(
        "pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"]
    )


@pytest.fixture
def mock_get_drivers(monkeypatch):
    monkeypatch.setattr(
        Db,
        "_Db__get_list_of_available_and_supported_pyodbc_drivers",
        lambda self: ["Driver1"],
    )


@pytest.fixture
def dwh_instance(mock_pyodbc_connect, mock_pyodbc_drivers):
    with patch.object(DWH, "_DWH__initialize_context_services"):
        return DWH(grs(), grs(), grs(), grs())


@pytest.fixture
def mock_iter_modules():
    with patch("pkgutil.iter_modules") as mock_iter_modules:
        yield mock_iter_modules


@pytest.fixture
def db_instance(mock_pyodbc_connect, mock_pyodbc_drivers):
    return Db(grs(), grs(), grs(), grs())


@pytest.fixture
def mock_error_response():
    response = AsyncMock()
    response.text = AsyncMock(return_value="Error Message")
    response.raise_for_status.side_effect = ClientResponseError(
        request_info=AsyncMock(), history=(), status=0, message="Error Message"
    )
    return response
