import pytest
import random
import string
import pyodbc
import logging
import datetime
from unittest.mock import Mock
from pyprediktormapclient.dwh import DWH

"""
Helpers
"""


class mock_pyodbc_connection:
    def __init__(self, connection_string):
        pass

    def cursor(self):
        return


def mock_pyodbc_connection_throws_error_not_tolerant_to_attempts(connection_string):
    raise pyodbc.DataError("Error code", "Error message")


def mock_pyodbc_connection_throws_error_tolerant_to_attempts(connection_string):
    raise pyodbc.DatabaseError("Error code", "Error message")


def grs():
    """Generate a random string."""
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


"""
__init__
"""


def test_init_when_instantiate_dwh_then_instance_is_created(monkeypatch):
    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktormapclient.dwh.db.pyodbc.connect", mock_pyodbc_connection
    )

    dwh = DWH(grs(), grs(), grs(), grs())
    assert dwh is not None
    assert dwh.plant is not None
    assert dwh.solcast is not None
    assert dwh.enercast is not None


def test_init_when_instantiate_dwh_but_no_pyodbc_drivers_available_then_throw_exception(
    monkeypatch,
):
    # Mock the absence of ODBC drivers
    monkeypatch.setattr("pyprediktormapclient.dwh.db.pyodbc.drivers", lambda: [])

    with pytest.raises(ValueError) as excinfo:
        DWH(grs(), grs(), grs(), grs())
    assert "Driver index 0 is out of range." in str(excinfo.value)


def test_init_when_instantiate_dwh_but_pyodbc_throws_error_with_tolerance_to_attempts_then_throw_exception(
    monkeypatch,
):
    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktormapclient.dwh.db.pyodbc.connect",
        mock_pyodbc_connection_throws_error_not_tolerant_to_attempts,
    )

    with pytest.raises(pyodbc.DataError):
        DWH(grs(), grs(), grs(), grs())


def test_init_when_instantiate_dwh_but_pyodbc_throws_error_tolerant_to_attempts_then_retry_connecting_and_throw_exception(
    caplog, monkeypatch
):
    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktormapclient.dwh.db.pyodbc.connect",
        mock_pyodbc_connection_throws_error_tolerant_to_attempts,
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(pyodbc.DatabaseError):
            DWH(grs(), grs(), grs(), grs())

    assert any(
        "Failed to connect to the DataWarehouse after 3 attempts." in message
        for message in caplog.messages
    )


"""
version
"""


def test_version_when_version_data_is_returned_then_return_version_data(monkeypatch):
    data_returned_by_dwh = [
        (
            "2.3.1",
            datetime.datetime(2023, 11, 14, 7, 5, 19, 830000),
            "Updated DWH from procs",
            2,
            3,
            1,
        )
    ]

    expected_query = "SET NOCOUNT ON; EXEC [dbo].[GetVersion]"
    expected_result = {
        "DWHVersion": "2.3.1",
        "UpdateDate": datetime.datetime(2023, 11, 14, 7, 5, 19, 830000),
        "Comment": "Updated DWH from procs",
        "MajorVersionNo": 2,
        "MinorVersionNo": 3,
        "InterimVersionNo": 1,
    }

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = data_returned_by_dwh
    mock_cursor.nextset.return_value = False
    mock_cursor.description = [
        ("DWHVersion", None),
        ("UpdateDate", None),
        ("Comment", None),
        ("MajorVersionNo", None),
        ("MinorVersionNo", None),
        ("InterimVersionNo", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    dwh = DWH(grs(), grs(), grs(), grs(), 2)
    version = dwh.version()

    mock_cursor.execute.assert_called_once_with(expected_query)
    assert version == expected_result


def test_version_when_version_data_is_not_returned_then_return_empty_tuple(monkeypatch):
    expected_query = "SET NOCOUNT ON; EXEC [dbo].[GetVersion]"
    expected_result = {}

    # Mock the cursor's fetchall methods
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.nextset.return_value = False
    mock_cursor.description = [
        ("DWHVersion", None),
        ("UpdateDate", None),
        ("Comment", None),
        ("MajorVersionNo", None),
        ("MinorVersionNo", None),
        ("InterimVersionNo", None),
    ]

    # Mock the connection method to return a mock connection with a mock cursor
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"])

    dwh = DWH(grs(), grs(), grs(), grs(), 2)
    version = dwh.version()

    mock_cursor.execute.assert_called_once_with(expected_query)
    assert version == expected_result
