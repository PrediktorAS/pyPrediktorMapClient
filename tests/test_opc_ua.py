import requests
import unittest
from unittest import mock
from pandas.testing import assert_frame_equal
import pandas as pd

from pyprediktormapclient.opc_ua import OPC_UA

URL = "http://someserver.somedomain.com/v1/"
OPC_URL = "opc.tcp://nosuchserver.nosuchdomain.com"

values_get_result = [
    {
        "Success": True,
        "ErrorMessage": "",
        "ErrorCode": 0,
        "ServerNamespaces": ["string"],
        "Values": [
            {
                "Value": {"Type": 0, "Body": "string"},
                "StatusCode": {"Code": 0, "Symbol": "string"},
                "SourceTimestamp": "2022-09-21T13:13:38.183Z",
                "SourcePicoseconds": 0,
                "ServerTimestamp": "2022-09-21T13:13:38.183Z",
                "ServerPicoseconds": 0,
            }
        ],
    }
]

correct_live_df = pd.DataFrame({"Id": ["1", "2"], "AnotherColumnName": [40, 34]})

# This method will be used by the mock to replace requests.get
def mocked_requests(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.raise_for_status = mock.Mock(return_value=False)

        def json(self):
            return self.json_data

    if args[0] == f"{URL}values/get":
        return MockResponse(values_get_result, 200)

    return MockResponse(None, 404)


# Our test case class
class OPCUATestCase(unittest.TestCase):
    @mock.patch("requests.post", side_effect=mocked_requests)
    def test_get_live_values(self, mock_get):

        pass  # wait with this test until dataframes can be removed
        # tsdata = OPC_UA(rest_url=URL, opcua_url= OPC_URL)
        # result = tsdata.get_live_values_data(['AngleMeasured', 'AngleSetpoint'], correct_live_df)
        # assert result == values_get_result


if __name__ == "__main__":
    unittest.main()
