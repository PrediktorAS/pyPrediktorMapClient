import pytest
import random
import string
from pyprediktormapclient.dwh import DWH

def grs():
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def test_database_operations_init_success(monkeypatch):
    # Mock the database connection
    def mock_connect(self):
        pass

    monkeypatch.setattr(
        "pyprediktormapclient.dwh.DWH.connect", mock_connect
    )

    dwh = DWH(grs(), grs(), grs(), grs())
    assert dwh is not None


def test_connect_to_database_using_pyodbc_no_drivers(monkeypatch):
    # Mock the absence of ODBC drivers
    monkeypatch.setattr("pyprediktormapclient.dwh.pyodbc.drivers", lambda: [])

    # Test failure scenario when no ODBC drivers are available
    with pytest.raises(ValueError) as excinfo:
        DWH(grs(), grs(), grs(), grs())
    assert "is out of range" in str(excinfo.value)
