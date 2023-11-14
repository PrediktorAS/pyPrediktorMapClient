import pytest
import random
import string
from pyprediktormapclient.dwh import DWH

'''
Helpers
'''
class mock_pyodbc_connection:
        def __init__(self, connection_string):
            pass

        def cursor(self):
            pass

def grs():
    '''Generate a random string.'''
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

'''
__init__
'''
def test_init_when_instantiate_dwh_then_instance_is_created(monkeypatch):
    # Mock the database connection
    monkeypatch.setattr(
        "pyprediktormapclient.dwh.pyodbc.connect",
        mock_pyodbc_connection
    )

    dwh = DWH(grs(), grs(), grs(), grs())
    assert dwh is not None

def test_init_when_instantiate_dwh_but_no_pyodbc_drivers_available_then_throw_exception(monkeypatch):
    # Mock the absence of ODBC drivers
    monkeypatch.setattr("pyprediktormapclient.dwh.pyodbc.drivers", lambda: [])

    with pytest.raises(ValueError) as excinfo:
        DWH(grs(), grs(), grs(), grs())
    assert "Driver index 0 is out of range." in str(excinfo.value)
