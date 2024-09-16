import pytest
from unittest.mock import AsyncMock
from aiohttp import ClientResponseError
from pyprediktormapclient.opc_ua import OPC_UA

URL = "http://someserver.somedomain.com/v1/"
OPC_URL = "opc.tcp://nosuchserver.nosuchdomain.com"


@pytest.fixture
def opc():
    opc = OPC_UA(rest_url=URL, opcua_url=OPC_URL)
    return opc


@pytest.fixture
def mock_error_response():
    response = AsyncMock()
    response.text = AsyncMock(return_value="Error Message")
    response.raise_for_status.side_effect = ClientResponseError(
        request_info=AsyncMock(), history=(), status=0, message="Error Message"
    )
    return response
