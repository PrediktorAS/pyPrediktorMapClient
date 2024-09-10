import pytest
import random
import string
import pyodbc
import logging
import pandas as pd
from unittest.mock import Mock, patch
from pyprediktormapclient.dwh.db import Db
from pandas.testing import assert_frame_equal

class TestCaseDB:
    @staticmethod
    def grs():
        """Generate a random string."""
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

    @pytest.fixture
    def mock_pyodbc_connect(self, monkeypatch):
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr('pyodbc.connect', lambda *args, **kwargs: mock_connection)
        return mock_cursor

    @pytest.fixture
    def mock_pyodbc_drivers(self, monkeypatch):
        monkeypatch.setattr('pyodbc.drivers', lambda: ['Driver1', 'Driver2', 'Driver3'])

    @pytest.fixture
    def mock_get_drivers(self, monkeypatch):
        monkeypatch.setattr(Db, '_Db__get_list_of_available_and_supported_pyodbc_drivers', lambda self: ['Driver1'])

    @pytest.fixture
    def db_instance(self, mock_pyodbc_connect, mock_pyodbc_drivers):
        return Db(self.grs(), self.grs(), self.grs(), self.grs())

    def test_init_successful(self, db_instance):
        assert db_instance is not None
        assert db_instance.driver == 'Driver1'

    def test_init_no_drivers(self, monkeypatch):
        monkeypatch.setattr('pyodbc.drivers', lambda: [])
        monkeypatch.setattr(Db, '_Db__get_list_of_available_and_supported_pyodbc_drivers', lambda self: [])

        with pytest.raises(ValueError, match="No supported ODBC drivers found."):
            Db(self.grs(), self.grs(), self.grs(), self.grs())

    @pytest.mark.parametrize('error, expected_error, expected_message', [
        (pyodbc.DataError('Error code', 'Error message'), pyodbc.DataError, 'Data Error Error code: Error message'),
        (pyodbc.DatabaseError('Error code', 'Error message'), pyodbc.DatabaseError, 'Failed to connect to the DataWarehouse after 3 attempts.'),
    ])
    def test_init_connection_error(self, monkeypatch, error, expected_error, expected_message, caplog, mock_get_drivers):
        monkeypatch.setattr('pyodbc.connect', Mock(side_effect=error))
        with pytest.raises(expected_error):
            with caplog.at_level(logging.ERROR):
                Db(self.grs(), self.grs(), self.grs(), self.grs())
        assert expected_message in caplog.text

    def test_init_when_instantiate_db_but_no_pyodbc_drivers_available_then_throw_exception(
        self, monkeypatch
    ):
        driver_index = 0
        monkeypatch.setattr('pyodbc.drivers', lambda: ['DRIVER1'])
        monkeypatch.setattr(Db, '_Db__get_list_of_available_and_supported_pyodbc_drivers', lambda self: [])

        with pytest.raises(ValueError, match="No supported ODBC drivers found."):
            Db(self.grs(), self.grs(), self.grs(), self.grs(), driver_index)

    def test_init_with_out_of_range_driver_index(self, monkeypatch):
        driver_index = 1
        monkeypatch.setattr('pyodbc.drivers', lambda: ['DRIVER1'])
        monkeypatch.setattr(Db, '_Db__get_list_of_available_and_supported_pyodbc_drivers', lambda self: ['DRIVER1'])

        with pytest.raises(ValueError, match="Driver index 1 is out of range."):
            Db(self.grs(), self.grs(), self.grs(), self.grs(), driver_index)

    def test_fetch_connection_error(self, db_instance, monkeypatch):
        monkeypatch.setattr(db_instance, '_Db__connect', Mock(side_effect=pyodbc.DataError('Error', 'Connection Error')))
        with pytest.raises(pyodbc.DataError):
            db_instance.fetch("SELECT * FROM test_table")

    @pytest.mark.parametrize('to_dataframe, expected_result', [
        (False, []),
        (True, pd.DataFrame()),
    ])
    def test_fetch_no_data(self, db_instance, mock_pyodbc_connect, to_dataframe, expected_result):
        mock_pyodbc_connect.fetchall.return_value = []
        mock_pyodbc_connect.nextset.return_value = False
        mock_pyodbc_connect.description = [("column1", None), ("column2", None)]

        result = db_instance.fetch("SELECT * FROM test_table", to_dataframe)

        if to_dataframe:
            assert result.empty
        else:
            assert result == expected_result

    @pytest.mark.parametrize('to_dataframe', [False, True])
    def test_fetch_single_dataset(self, db_instance, mock_pyodbc_connect, to_dataframe):
        data = [("value1", 1), ("value2", 2)]
        mock_pyodbc_connect.fetchall.return_value = data
        mock_pyodbc_connect.nextset.return_value = False
        mock_pyodbc_connect.description = [("column1", None), ("column2", None)]

        result = db_instance.fetch("SELECT * FROM test_table", to_dataframe)

        if to_dataframe:
            expected = pd.DataFrame(data, columns=["column1", "column2"])
            assert_frame_equal(result, expected)
        else:
            expected = [{"column1": "value1", "column2": 1}, {"column1": "value2", "column2": 2}]
            assert result == expected

    @pytest.mark.parametrize('to_dataframe', [False, True])
    def test_fetch_multiple_datasets(self, db_instance, mock_pyodbc_connect, to_dataframe):
        data1 = [("value1", 1), ("value2", 2)]
        data2 = [("value3", 3), ("value4", 4)]
        mock_pyodbc_connect.fetchall.side_effect = [data1, data2]
        mock_pyodbc_connect.nextset.side_effect = [True, False]
        mock_pyodbc_connect.description = [("column1", None), ("column2", None)]

        result = db_instance.fetch("SELECT * FROM test_table", to_dataframe)

        if to_dataframe:
            expected1 = pd.DataFrame(data1, columns=["column1", "column2"])
            expected2 = pd.DataFrame(data2, columns=["column1", "column2"])
            assert len(result) == 2
            assert_frame_equal(result[0], expected1)
            assert_frame_equal(result[1], expected2)
        else:
            expected = [
                [{"column1": "value1", "column2": 1}, {"column1": "value2", "column2": 2}],
                [{"column1": "value3", "column2": 3}, {"column1": "value4", "column2": 4}]
            ]
            assert result == expected

    def test_execute_connection_error(self, db_instance, monkeypatch):
        monkeypatch.setattr(db_instance, '_Db__connect', Mock(side_effect=pyodbc.Error('Error', 'Connection Error')))
        with pytest.raises(pyodbc.Error):
            db_instance.execute("INSERT INTO test_table VALUES (1, 'test')")

    def test_execute_with_parameters(self, db_instance, mock_pyodbc_connect):
        query = "INSERT INTO test_table VALUES (?, ?)"
        params = ("John", "Smith")
        expected_result = [{"id": 13}]
        mock_pyodbc_connect.fetchall.return_value = expected_result

        result = db_instance.execute(query, *params)

        mock_pyodbc_connect.execute.assert_called_once_with(query, *params)
        mock_pyodbc_connect.fetchall.assert_called_once()
        assert result == expected_result

    def test_execute_fetchall_error(self, db_instance, mock_pyodbc_connect):
        query = "INSERT INTO test_table VALUES (?, ?)"
        params = ("John", "Smith")
        mock_pyodbc_connect.fetchall.side_effect = Exception("Error occurred")

        result = db_instance.execute(query, *params)

        mock_pyodbc_connect.execute.assert_called_once_with(query, *params)
        mock_pyodbc_connect.fetchall.assert_called_once()
        assert result == []

    def test_context_manager_enter(self, db_instance):
        assert db_instance.__enter__() == db_instance

    def test_context_manager_exit(self, db_instance, monkeypatch):
        disconnect_called = False
        def mock_disconnect():
            nonlocal disconnect_called
            disconnect_called = True
        
        monkeypatch.setattr(db_instance, '_Db__disconnect', mock_disconnect)
        db_instance.connection = True
        
        db_instance.__exit__(None, None, None)
        assert disconnect_called

    def test_set_driver_with_valid_index(self, monkeypatch, db_instance):
        available_drivers = ['DRIVER1', 'DRIVER2']
        monkeypatch.setattr(Db, '_Db__get_list_of_available_and_supported_pyodbc_drivers', lambda self: available_drivers)
        
        db_instance._Db__set_driver(1)
        assert db_instance.driver == 'DRIVER2'

    def test_get_number_of_available_pyodbc_drivers(self, db_instance, monkeypatch):
        monkeypatch.setattr(db_instance, '_Db__get_list_of_supported_pyodbc_drivers', lambda: ['DRIVER1', 'DRIVER2'])
        assert db_instance._Db__get_number_of_available_pyodbc_drivers() == 2

    def test_connect_success(self, db_instance, monkeypatch):
        connect_called = False
        def mock_connect(*args, **kwargs):
            nonlocal connect_called
            connect_called = True
            return Mock(cursor=Mock())

        with patch('pyprediktormapclient.dwh.db.pyodbc.connect', side_effect=mock_connect) as mock:
            db_instance._Db__connect()
            assert mock.called
            assert connect_called

    def test_connect_retry_on_operational_error(self, db_instance, monkeypatch):
        attempt_count = 0
        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.OperationalError('Test error')
            return Mock(cursor=Mock())

        with patch('pyprediktormapclient.dwh.db.pyodbc.connect', side_effect=mock_connect) as mock:
            db_instance._Db__connect()
            assert mock.call_count == 3

    def test_connect_raise_on_integrity_error(self, db_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.IntegrityError('Test integrity error')

        with patch('pyprediktormapclient.dwh.db.pyodbc.connect', side_effect=mock_connect):
            with pytest.raises(pyodbc.IntegrityError):
                db_instance._Db__connect()

    def test_connect_raise_on_programming_error(self, db_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.ProgrammingError('Test programming error')

        with patch('pyodbc.connect', side_effect=mock_connect):
            with pytest.raises(pyodbc.ProgrammingError):
                db_instance._Db__connect()

    def test_connect_raise_on_not_supported_error(self, db_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.NotSupportedError('Test not supported error')

        with patch('pyodbc.connect', side_effect=mock_connect):
            with pytest.raises(pyodbc.NotSupportedError):
                db_instance._Db__connect()

    def test_connect_retry_on_generic_error(self, db_instance, monkeypatch):
        attempt_count = 0
        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.Error('Test generic error')
            return Mock(cursor=Mock())

        with patch('pyprediktormapclient.dwh.db.pyodbc.connect', side_effect=mock_connect) as mock:
            db_instance._Db__connect()
            assert mock.call_count == 3

    def test_disconnect(self, db_instance):
        mock_connection = Mock()
        db_instance.connection = mock_connection
        db_instance.cursor = Mock()

        db_instance._Db__disconnect()

        assert mock_connection.close.called
        assert db_instance.connection is None
        assert db_instance.cursor is None