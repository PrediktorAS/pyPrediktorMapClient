import pytest
import random
import string
import pyodbc
import logging
from pyprediktormapclient.dwh import DWH

"""
Helpers
"""


class mock_pyodbc_connection:
    def __init__(self, connection_string):
        pass

    def cursor(self):
        pass


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
        "pyprediktormapclient.dwh.pyodbc.connect", mock_pyodbc_connection
    )

    dwh = DWH(grs(), grs(), grs(), grs())
    assert dwh is not None


def test_init_when_instantiate_dwh_but_no_pyodbc_drivers_available_then_throw_exception(
    monkeypatch,
):
    # Mock the absence of ODBC drivers
    monkeypatch.setattr("pyprediktormapclient.dwh.pyodbc.drivers", lambda: [])

    with pytest.raises(ValueError) as excinfo:
        DWH(grs(), grs(), grs(), grs())
    assert "Driver index 0 is out of range." in str(excinfo.value)


def test_init_when_instantiate_dwh_but_pyodbc_throws_error_with_tolerance_to_attempts_then_throw_exception(
    monkeypatch,
):
    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktormapclient.dwh.pyodbc.connect",
        mock_pyodbc_connection_throws_error_not_tolerant_to_attempts,
    )

    with pytest.raises(pyodbc.DataError):
        DWH(grs(), grs(), grs(), grs())


def test_init_when_instantiate_dwh_but_pyodbc_throws_error_tolerant_to_attempts_then_retry_connecting_and_throw_exception(
    caplog, monkeypatch
):
    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktormapclient.dwh.pyodbc.connect",
        mock_pyodbc_connection_throws_error_tolerant_to_attempts,
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(pyodbc.DatabaseError):
            DWH(grs(), grs(), grs(), grs())

    assert any(
        "Failed to connect to the DataWarehouse after 3 attempts." in message
        for message in caplog.messages
    )
