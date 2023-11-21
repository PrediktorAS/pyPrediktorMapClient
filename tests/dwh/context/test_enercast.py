import json
from unittest.mock import Mock
from pyprediktormapclient.dwh.idwh import IDWH
from pyprediktormapclient.dwh.context.enercast import Enercast

"""
__init__
"""


def test_init(monkeypatch):
    mock_dwh = Mock(spec=IDWH)

    enercast = Enercast(mock_dwh)
    assert enercast.dwh == mock_dwh


"""
get_plants_to_update
"""


def test_get_plants_to_update(monkeypatch):
    expected_query = "SET NOCOUNT ON; EXEC dwetl.GetEnercastPlantsToUpdate"
    expected_result = [
        {
            "plantname": "CC-LL",
            "AssetName": "PredPV1",
            "AssetId": "86358efb-c076-4bca-a775-0a63ec92d8ba",
            "Percentile": 20,
            "ExtForecastTypeKey": 11,
        },
        {
            "plantname": "CC-LL",
            "AssetName": "PredPV1",
            "AssetId": "86358efb-c076-4bca-a775-0a63ec92d8ba",
            "Percentile": 80,
            "ExtForecastTypeKey": 12,
        },
    ]

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.fetch.return_value = expected_result

    enercast = Enercast(mock_dwh)
    actual_result = enercast.get_plants_to_update()

    mock_dwh.fetch.assert_called_once_with(expected_query)
    assert actual_result == expected_result


"""
get_live_meter_data
"""


def test_get_live_meter_data(monkeypatch):
    asset_name = "PredPV1"

    expected_query = (
        f"SET NOCOUNT ON; EXEC dwetl.GetEnercastLiveMeterData '{asset_name}'"
    )
    expected_result = [
        {
            "Datetime UTC interval start (15m interval preferred)": "2023-11-20 16:00:00",
            "Power Output Limit (kW) (e.g. Grid-/Curtailment/Inverter AC Limit)": 3250.0,
        },
        {
            "Datetime UTC interval start (15m interval preferred)": "2023-11-20 17:00:00",
            "Power Output Limit (kW) (e.g. Grid-/Curtailment/Inverter AC Limit)": 3250.0,
        },
    ]

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.fetch.return_value = expected_result

    enercast = Enercast(mock_dwh)
    actual_result = enercast.get_live_meter_data(asset_name)

    mock_dwh.fetch.assert_called_once_with(expected_query)
    assert actual_result == expected_result


"""
upsert_forecast_data
"""


def test_upsert_forecast_data(monkeypatch):
    enercast_forecast_data = {
        "results": {
            "facilityName": "PredPV1",
            "powerUnit": "W",
            "values": [
                {"timestamp": "2023-11-20T17:30:00Z", "normed": 0, "absolute": 0},
                {"timestamp": "2023-11-20T17:45:00Z", "normed": 0, "absolute": 0},
                {"timestamp": "2023-11-20T18:00:00Z", "normed": 0, "absolute": 0},
            ],
        }
    }
    forecast_type_key = 1

    enercast_forecast_data_json = json.dumps({"results": enercast_forecast_data})
    expected_query = f"EXEC dwetl.UpsertEnercastForecastData {enercast_forecast_data_json}, {forecast_type_key}"
    expected_result = []

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.execute.return_value = expected_result

    enercast = Enercast(mock_dwh)
    actual_result = enercast.upsert_forecast_data(
        enercast_forecast_data, forecast_type_key
    )

    mock_dwh.execute.assert_called_once_with(expected_query)
    assert actual_result == expected_result
