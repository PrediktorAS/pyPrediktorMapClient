import unittest
import pytest

from pyprediktormapclient.shared import request_from_api


class AnalyticsHelperTestCase(unittest.TestCase):
    def test_requests_with_unsupported_method(self):
        with pytest.raises(Exception):
            request_from_api(rest_url="No_valid_url", method="PUT", endpoint="/")


if __name__ == "__main__":
    unittest.main()
