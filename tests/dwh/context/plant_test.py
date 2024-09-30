import json
from unittest.mock import Mock

from pyprediktormapclient.dwh.context.plant import Plant
from pyprediktormapclient.dwh.idwh import IDWH

"""
__init__
"""


def test_init(monkeypatch):
    mock_dwh = Mock(spec=IDWH)

    plant = Plant(mock_dwh)
    assert plant.dwh == mock_dwh


"""
get_optimal_tracker_angles
"""


def test_get_optimal_tracker_angles(monkeypatch):
    facility_name = "AnotherPlant"

    expected_query = (
        "SET NOCOUNT ON; EXEC dwetl.GetOptimalTrackerAngleParameters "
        f"@FacilityName = N'{facility_name}'"
    )
    expected_result = []

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.fetch.return_value = expected_result

    plant = Plant(mock_dwh)
    actual_result = plant.get_optimal_tracker_angles(facility_name)

    mock_dwh.fetch.assert_called_once_with(expected_query)
    assert actual_result == expected_result


"""
upsert_optimal_tracker_angles
"""


def test_upsert_optimal_tracker_angles(monkeypatch):
    facility_data = {"key": "value"}
    facility_data_json = json.dumps(facility_data)

    facility_data_json.replace("'", '"')
    expected_query = "EXEC dwetl.UpsertOptimalTrackerAngles @json = ?"
    expected_result = []

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.execute.return_value = expected_result

    plant = Plant(mock_dwh)
    actual_result = plant.upsert_optimal_tracker_angles(facility_data)

    mock_dwh.execute.assert_called_once_with(
        expected_query, facility_data_json
    )
    assert actual_result == expected_result


"""
insert_log
"""


def test_insert_log_when_has_thrown_error_is_false_then_result_is_ok(
    monkeypatch,
):
    message = "Some message"
    plantname = "XY-ZK2"
    data_type = "Forecast"
    has_thrown_error = False
    ext_forecast_type_key = 11

    expected_query = (
        "EXEC dwetl.InsertExtDataUpdateLog "
        "@plantname = ?, "
        "@extkey = ?, "
        "@DataType = ?, "
        "@Message = ?, "
        "@Result = ?"
    )
    expected_result = []

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.execute.return_value = expected_result

    plant = Plant(mock_dwh)
    actual_result = plant.insert_log(
        plantname, ext_forecast_type_key, data_type, has_thrown_error, message
    )

    mock_dwh.execute.assert_called_once_with(
        expected_query,
        plantname,
        ext_forecast_type_key,
        data_type,
        message,
        "OK",
    )
    assert actual_result == expected_result


def test_insert_log_when_has_thrown_error_is_true_then_result_is_error(
    monkeypatch,
):
    message = "Some message"
    plantname = "XY-ZK2"
    data_type = "Forecast"
    has_thrown_error = True
    ext_forecast_type_key = 11

    expected_query = (
        "EXEC dwetl.InsertExtDataUpdateLog "
        "@plantname = ?, "
        "@extkey = ?, "
        "@DataType = ?, "
        "@Message = ?, "
        "@Result = ?"
    )
    expected_result = []

    mock_dwh = Mock(spec=IDWH)
    mock_dwh.execute.return_value = expected_result

    plant = Plant(mock_dwh)
    actual_result = plant.insert_log(
        plantname, ext_forecast_type_key, data_type, has_thrown_error, message
    )

    mock_dwh.execute.assert_called_once_with(
        expected_query,
        plantname,
        ext_forecast_type_key,
        data_type,
        message,
        "ERROR",
    )
    assert actual_result == expected_result
