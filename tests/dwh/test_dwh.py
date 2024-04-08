import pytest
import random
import string
import pyodbc
import logging
import datetime
from unittest.mock import Mock
from pyprediktormapclient.dwh.dwh import DWH

"""
Mock Functions
"""

def mock_pyodbc_connection_throws_error_not_tolerant_to_attempts(connection_string):
    raise pyodbc.DataError("Error code", "Error message")

def mock_pyodbc_connection_throws_error_tolerant_to_attempts(connection_string):
    def attempt_connect():
        if attempt_connect.counter < 3:
            attempt_connect.counter += 1
            raise pyodbc.DatabaseError("Error code", "Temporary error message")
        else:
            raise pyodbc.DatabaseError("Error code", "Permanent error message")
    attempt_connect.counter = 0
    return attempt_connect()

"""
Helper Function
"""

def grs():
    """Generate a random string suitable for URL, database, username, password."""
    return "test_string"

"""
Test Functions
"""


def test_init_when_instantiate_dwh_but_pyodbc_throws_error_with_tolerance_to_attempts_then_throw_exception(
    monkeypatch,
):
    driver_index = 0

    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktorutilities.dwh.dwh.pyodbc.connect",
        mock_pyodbc_connection_throws_error_not_tolerant_to_attempts,
    )

    with pytest.raises(pyodbc.DataError):
        DWH(grs(), grs(), grs(), grs(), driver_index)


def test_init_when_instantiate_dwh_but_pyodbc_throws_error_tolerant_to_attempts_then_retry_connecting_and_throw_exception(
    caplog, monkeypatch
):
    driver_index = 0

    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktorutilities.dwh.dwh.pyodbc.connect",
        mock_pyodbc_connection_throws_error_tolerant_to_attempts,
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(pyodbc.DatabaseError):
            DWH(grs(), grs(), grs(), grs(), driver_index)

    assert any(
        "Failed to connect to the DataWarehouse after 3 attempts." in message
        for message in caplog.messages
    )


def test_init_when_instantiate_dwh_but_driver_index_is_not_passed_then_instance_is_created(
    monkeypatch,
):
    # Mock the connection method to return a mock connection with a mock cursor
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    monkeypatch.setattr("pyodbc.connect", lambda *args, **kwargs: mock_connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["Driver1", "Driver2"])

    dwh = DWH(grs(), grs(), grs(), grs())
    assert dwh is not None
    assert dwh.driver == "Driver1"


"""
version
"""


def test_version_when_version_data_is_returned_then_return_version_data(monkeypatch):
    driver_index = 2
    data_returned_by_dwh = [
        (
            "2.3.1",
            datetime.datetime(2023, 11, 14, 7, 5, 19, 830000),
            "Updated Dwh from procs",
            2,
            3,
            1,
        )
    ]

    expected_query = "SET NOCOUNT ON; EXEC [dbo].[GetVersion]"
    expected_result = {
        "DWHVersion": "2.3.1",
        "UpdateDate": datetime.datetime(2023, 11, 14, 7, 5, 19, 830000),
        "Comment": "Updated Dwh from procs",
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

    dwh = DWH(grs(), grs(), grs(), grs(), driver_index)
    version = dwh.version()

    mock_cursor.execute.assert_called_once_with(expected_query)
    assert version == expected_result


def test_version_when_version_data_is_not_returned_then_return_empty_tuple(monkeypatch):
    driver_index = 2
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

    dwh = DWH(grs(), grs(), grs(), grs(), driver_index)
    version = dwh.version()

    mock_cursor.execute.assert_called_once_with(expected_query)
    assert version == expected_result
