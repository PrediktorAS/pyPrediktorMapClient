import unittest
from unittest import mock
from pandas.testing import assert_frame_equal

from pyprediktormapclient.shared import normalize_as_dataframe

objects_of_type = [
    {
        "Id": "3:1:SSO.EG-AS",
        "Type": "6:0:1009",
        "Subtype": "6:0:1009",
        "DisplayName": "EG-AS",
        "Props": [{"DisplayName": "GPSLatitude", "Value": "24.44018"}],
        "Vars": [
            {
                "DisplayName": "Alarm.CommLossPlantDevice",
                "Id": "3:1:SSO.EG-AS.Signals.Alarm.CommLossPlantDevice",
            }
        ],
    }
]

# Our test case class
class SharedTestCase(unittest.TestCase):
    def test_get_object_types_as_json(self):
        result = normalize_as_dataframe(objects_of_type)
        assert "DisplayName" not in result.columns
        assert "Name" in result.columns

if __name__ == "__main__":
    unittest.main()
