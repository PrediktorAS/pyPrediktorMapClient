import json
from unittest.mock import Mock

from pyprediktormapclient.dwh.context.solcast import Solcast
from pyprediktormapclient.dwh.idwh import IDWH

"""
__init__
"""


def test_init(monkeypatch):
    mock_dwh = Mock(spec=IDWH)

    solcast = Solcast(mock_dwh)
    assert solcast.dwh == mock_dwh


"""
get_plants_to_update
"""


def test_get_plants_to_update(monkeypatch):
    expected_query = "SET NOCOUNT ON; EXEC dwetl.GetSolcastPlantsToUpdate"
    expected_result = [
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "XY-ZK",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 14,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
        {
            "plantname": "KL-MN",
            "resource_id": "1234-abcd-efgh-5678",
            "api_key": "SOME_KEY",
            "ExtForecastTypeKey": 13,
            "hours": 168,
            "output_parameters": "pv_power_advanced",
            "period": "PT15M",
        },
    ]

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.fetch.return_value = expected_result

    solcast = Solcast(mock_dwh)
    actual_result = solcast.get_plants_to_update()

    mock_dwh.fetch.assert_called_once_with(expected_query)
    assert actual_result == expected_result


"""
upsert_forecast_data
"""


def test_upsert_forecast_data(monkeypatch):
    plantname = "SomePlant"
    solcast_forecast_data = {"forecasts": {"key": "value"}}
    forecast_type_key = 1

    solcast_forecast_data_json = json.dumps(
        {
            "results": {
                "plantname": plantname,
                "values": solcast_forecast_data["forecasts"],
            }
        }
    )
    expected_query = "EXEC dwetl.UpsertSolcastForecastData ?, ?"
    expected_result = {
        "data": [
            {
                "pv_power_advanced": 5.714,
                "period_end": "2023-11-27T15:45:00.0000000Z",
                "period": "PT15M",
            },
            {
                "pv_power_advanced": 7.62,
                "period_end": "2023-11-27T16:00:00.0000000Z",
                "period": "PT15M",
            },
            {
                "pv_power_advanced": 8.887,
                "period_end": "2023-11-27T16:15:00.0000000Z",
                "period": "PT15M",
            },
            {
                "pv_power_advanced": 10.05,
                "period_end": "2023-11-27T16:30:00.0000000Z",
                "period": "PT15M",
            },
            {
                "pv_power_advanced": 11.125,
                "period_end": "2023-11-27T16:45:00.0000000Z",
                "period": "PT15M",
            },
        ]
    }

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.execute.return_value = expected_result

    solcast = Solcast(mock_dwh)
    actual_result = solcast.upsert_forecast_data(
        plantname, solcast_forecast_data, forecast_type_key
    )

    mock_dwh.execute.assert_called_once_with(
        expected_query, solcast_forecast_data_json, forecast_type_key
    )
    assert actual_result == expected_result
