import pytest
import random
import string
import pyodbc
import inspect
import logging
import pandas as pd
from unittest.mock import Mock, patch
from typing import List, Any, get_origin, get_args
from pyprediktormapclient.dwh.db import Db
from pyprediktormapclient.dwh.idwh import IDWH
from pandas.testing import assert_frame_equal


class TestCaseDB:
    @staticmethod
    def grs():
        """Generate a random string."""
        return "".join(
            random.choices(string.ascii_uppercase + string.digits, k=10)
        )

    @pytest.fixture
    def mock_pyodbc_connect(self, monkeypatch):
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        monkeypatch.setattr(
            "pyodbc.connect", lambda *args, **kwargs: mock_connection
        )
        return mock_cursor

    @pytest.fixture
    def mock_pyodbc_drivers(self, monkeypatch):
        monkeypatch.setattr(
            "pyodbc.drivers", lambda: ["Driver1", "Driver2", "Driver3"]
        )

    @pytest.fixture
    def mock_get_drivers(self, monkeypatch):
        monkeypatch.setattr(
            Db,
            "_Db__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: ["Driver1"],
        )

    @pytest.fixture
    def db_instance(self, mock_pyodbc_connect, mock_pyodbc_drivers):
        return Db(self.grs(), self.grs(), self.grs(), self.grs())

    @pytest.mark.parametrize(
        "error, expected_error, expected_log_message",
        [
            (
                pyodbc.DataError("Error code", "Error message"),
                pyodbc.DataError,
                "DataError Error code: Error message",
            ),
            (
                pyodbc.DatabaseError("Error code", "Error message"),
                pyodbc.Error,
                "DatabaseError Error code: Error message",
            ),
        ],
    )
    def test_init_connection_error(
        self,
        monkeypatch,
        error,
        expected_error,
        expected_log_message,
        caplog,
        mock_get_drivers,
    ):
        monkeypatch.setattr("pyodbc.connect", Mock(side_effect=error))
        with pytest.raises(expected_error) as exc_info:
            with caplog.at_level(logging.ERROR):
                Db(self.grs(), self.grs(), self.grs(), self.grs())

        if expected_error == pyodbc.Error:
            assert str(exc_info.value) == "Failed to connect to the database"
        else:
            assert str(exc_info.value) == str(error)

        assert expected_log_message in caplog.text
        if expected_error == pyodbc.Error:
            assert (
                "Failed to connect to the DataWarehouse after 3 attempts"
                in caplog.text
            )

    def test_init_when_instantiate_db_but_no_pyodbc_drivers_available_then_throw_exception(
        self, monkeypatch
    ):
        driver_index = 0
        monkeypatch.setattr("pyodbc.drivers", lambda: ["DRIVER1"])
        monkeypatch.setattr(
            Db,
            "_Db__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: [],
        )

        with pytest.raises(
            ValueError, match="No supported ODBC drivers found."
        ):
            Db(self.grs(), self.grs(), self.grs(), self.grs(), driver_index)

    def test_init_with_out_of_range_driver_index(self, monkeypatch):
        driver_index = 1
        monkeypatch.setattr("pyodbc.drivers", lambda: ["DRIVER1"])
        monkeypatch.setattr(
            Db,
            "_Db__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: ["DRIVER1"],
        )

        with pytest.raises(
            ValueError, match="Driver index 1 is out of range."
        ):
            Db(self.grs(), self.grs(), self.grs(), self.grs(), driver_index)

    def test_context_manager_enter(self, db_instance):
        assert db_instance.__enter__() == db_instance

    def test_exit_without_connection(self, db_instance):
        db_instance.connection = None
        db_instance.__exit__(None, None, None)

    def test_exit_with_open_connection_and_cleanup(self, db_instance):
        mock_connection = Mock()
        db_instance.connection = mock_connection
        db_instance.__exit__(None, None, None)

        mock_connection.close.assert_called_once()
        assert (
            db_instance.connection is None
        ), "Connection should be set to None after exit"

    def test_idwh_abstract_methods(self):
        assert inspect.isabstract(IDWH)
        assert set(IDWH.__abstractmethods__) == {"version", "fetch", "execute"}

    def test_db_implements_abstract_idwh(
        self, db_instance, mock_pyodbc_connect
    ):
        def compare_signatures(impl_method, abstract_method):
            if hasattr(impl_method, "__wrapped__"):
                impl_method = impl_method.__wrapped__

            impl_sig = inspect.signature(impl_method)
            abstract_sig = inspect.signature(abstract_method)

            impl_return = impl_sig.return_annotation
            abstract_return = abstract_sig.return_annotation

            impl_origin = get_origin(impl_return) or impl_return
            abstract_origin = get_origin(abstract_return) or abstract_return

            assert (
                impl_origin == abstract_origin
            ), f"Return type mismatch: {impl_return} is not compatible with {abstract_return}"

            if impl_origin is List:
                impl_args = get_args(impl_return)
                abstract_args = get_args(abstract_return)

                if not abstract_args:
                    abstract_args = (Any,)

                assert len(impl_args) == len(
                    abstract_args
                ), f"Generic argument count mismatch: {impl_args} vs {abstract_args}"
                for impl_arg, abstract_arg in zip(impl_args, abstract_args):
                    assert (
                        impl_arg == abstract_arg or abstract_arg == Any
                    ), f"Generic argument mismatch: {impl_arg} is not compatible with {abstract_arg}"

            impl_params = list(impl_sig.parameters.values())[1:]
            abstract_params = list(abstract_sig.parameters.values())[1:]

            assert len(impl_params) == len(
                abstract_params
            ), f"Parameter count mismatch: implementation has {len(impl_params)}, abstract has {len(abstract_params)}"

            for impl_param, abstract_param in zip(
                impl_params, abstract_params
            ):
                assert (
                    impl_param.name == abstract_param.name
                ), f"Parameter name mismatch: {impl_param.name} != {abstract_param.name}"
                assert (
                    impl_param.annotation == abstract_param.annotation
                ), f"Parameter type mismatch for {impl_param.name}: {impl_param.annotation} != {abstract_param.annotation}"
                assert (
                    impl_param.default == abstract_param.default
                ), f"Parameter default value mismatch for {impl_param.name}: {impl_param.default} != {abstract_param.default}"

        for method_name in ["fetch", "execute"]:
            assert hasattr(
                db_instance, method_name
            ), f"Db class is missing method: {method_name}"
            assert callable(
                getattr(db_instance, method_name)
            ), f"Db.{method_name} is not callable"
            compare_signatures(
                getattr(db_instance, method_name), getattr(IDWH, method_name)
            )

        mock_pyodbc_connect.description = [("column1",), ("column2",)]
        mock_pyodbc_connect.fetchall.return_value = [(1, "a"), (2, "b")]
        mock_pyodbc_connect.nextset.return_value = False

        fetch_result = db_instance.fetch("SELECT * FROM dummy_table")
        assert isinstance(
            fetch_result, list
        ), "fetch method should return a list"
        execute_result = db_instance.execute(
            "INSERT INTO dummy_table VALUES (1, 'test')"
        )
        assert isinstance(
            execute_result, list
        ), "execute method should return a list"

    def test_idwh_instantiation_raises_error(self):
        with pytest.raises(
            TypeError,
            match="Can't instantiate abstract class IDWH without an implementation for abstract methods 'execute', 'fetch', 'version'",
        ):
            IDWH()

    @pytest.mark.parametrize("to_dataframe", [False, True])
    def test_fetch_multiple_datasets(
        self, db_instance, mock_pyodbc_connect, to_dataframe
    ):
        data1 = [("value1", 1), ("value2", 2)]
        data2 = [("value3", 3), ("value4", 4)]
        mock_pyodbc_connect.fetchall.side_effect = [data1, data2]
        mock_pyodbc_connect.nextset.side_effect = [True, False]
        mock_pyodbc_connect.description = [
            ("column1", None),
            ("column2", None),
        ]

        result = db_instance.fetch("SELECT * FROM test_table", to_dataframe)

        if to_dataframe:
            expected1 = pd.DataFrame(data1, columns=["column1", "column2"])
            expected2 = pd.DataFrame(data2, columns=["column1", "column2"])
            assert len(result) == 2
            assert_frame_equal(result[0], expected1)
            assert_frame_equal(result[1], expected2)
        else:
            expected = [
                [
                    {"column1": "value1", "column2": 1},
                    {"column1": "value2", "column2": 2},
                ],
                [
                    {"column1": "value3", "column2": 3},
                    {"column1": "value4", "column2": 4},
                ],
            ]
            assert result == expected

    def test_execute_with_fetch_error(
        self, db_instance, mock_pyodbc_connect, caplog
    ):
        mock_pyodbc_connect.fetchall.side_effect = Exception("Fetch error")

        result = db_instance.execute("SELECT * FROM test_table")

        assert result == []
        assert "Failed to execute query: Fetch error" in caplog.text

    def test_set_driver_with_valid_index(self, monkeypatch, db_instance):
        available_drivers = ["DRIVER1", "DRIVER2"]
        monkeypatch.setattr(
            Db,
            "_Db__get_list_of_available_and_supported_pyodbc_drivers",
            lambda self: available_drivers,
        )

        db_instance._Db__set_driver(1)
        assert db_instance.driver == "DRIVER2"

    def test_get_list_of_supported_pyodbc_drivers_error(
        self, db_instance, monkeypatch, caplog
    ):
        monkeypatch.setattr(
            pyodbc, "drivers", Mock(side_effect=pyodbc.Error("Test error"))
        )

        result = db_instance._Db__get_list_of_supported_pyodbc_drivers()
        assert result == []
        assert "Error retrieving drivers: Test error" in caplog.text

    @patch("pyodbc.connect")
    def test_get_available_and_supported_drivers(
        self, mock_connect, db_instance
    ):
        db_instance.__get_list_of_supported_pyodbc_drivers = Mock(
            return_value=["Driver1", "Driver2", "Driver3"]
        )
        mock_connect.side_effect = [None, pyodbc.Error, None]

        result = (
            db_instance._Db__get_list_of_available_and_supported_pyodbc_drivers()
        )

        assert result == ["Driver1", "Driver3"]
        assert mock_connect.call_count == 3

    def test_connect_success(self, db_instance, monkeypatch):
        mock_connection = Mock()
        monkeypatch.setattr(
            "pyodbc.connect", lambda *args, **kwargs: mock_connection
        )

        db_instance.connection = None
        db_instance._Db__connect()

        assert db_instance.connection is mock_connection
        assert db_instance.cursor is not None

    def test_connect_raises_data_error(self, db_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.DataError("Data error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        db_instance.connection = None
        with pytest.raises(pyodbc.DataError):
            db_instance._Db__connect()

    def test_connect_raises_integrity_error(self, db_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.IntegrityError("Integrity error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        db_instance.connection = None
        with pytest.raises(pyodbc.IntegrityError):
            db_instance._Db__connect()

    def test_connect_raises_programming_error(self, db_instance, monkeypatch):
        def mock_connect(*args, **kwargs):
            raise pyodbc.ProgrammingError("Programming error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        db_instance.connection = None
        with pytest.raises(pyodbc.ProgrammingError):
            db_instance._Db__connect()

    def test_connect_raises_not_supported_error(
        self, db_instance, monkeypatch
    ):
        def mock_connect(*args, **kwargs):
            raise pyodbc.NotSupportedError("Not supported error")

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        db_instance.connection = None
        with pytest.raises(pyodbc.NotSupportedError):
            db_instance._Db__connect()

    def test_connect_retries_on_operational_error(
        self, db_instance, monkeypatch
    ):
        attempt_count = 0

        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.OperationalError("Operational error")
            return Mock()

        db_instance.connection_attempts = 3
        db_instance.connection = None

        with patch("pyodbc.connect", side_effect=mock_connect):
            db_instance._Db__connect()

        assert (
            attempt_count == 3
        ), "Should attempt three connections before succeeding"
        assert (
            db_instance.connection is not None
        ), "Connection should be established after retries"

    def test_connect_raises_after_max_attempts_on_operational_error(
        self, db_instance, monkeypatch
    ):
        def mock_connect(*args, **kwargs):
            raise pyodbc.OperationalError("Operational error")

        db_instance.connection = None
        db_instance.connection_attempts = 3
        with patch("pyodbc.connect", side_effect=mock_connect):
            with pytest.raises(
                pyodbc.Error, match="Failed to connect to the database"
            ):
                db_instance._Db__connect()

    def test_connect_retries_on_database_error(self, db_instance, monkeypatch):
        attempt_count = 0

        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.DatabaseError("Database error")
            return Mock()

        db_instance.connection_attempts = 3
        db_instance.connection = None

        with patch("pyodbc.connect", side_effect=mock_connect):
            db_instance._Db__connect()

        assert (
            attempt_count == 3
        ), "Should attempt three connections before succeeding"

    def test_connect_raises_after_max_attempts_on_database_error(
        self, db_instance, monkeypatch
    ):
        def mock_connect(*args, **kwargs):
            raise pyodbc.DatabaseError("Database error")

        db_instance.connection = None
        db_instance.connection_attempts = 3
        with patch("pyodbc.connect", side_effect=mock_connect):
            with pytest.raises(
                pyodbc.Error, match="Failed to connect to the database"
            ):
                db_instance._Db__connect()

    def test_connect_retries_on_generic_error(self, db_instance, monkeypatch):
        attempt_count = 0

        def mock_connect(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise pyodbc.Error("Generic error")
            return Mock()

        db_instance.connection_attempts = 3
        db_instance.connection = None

        with patch("pyodbc.connect", side_effect=mock_connect):
            db_instance._Db__connect()

        assert (
            attempt_count == 3
        ), "Should attempt three connections before succeeding"

    def test_connect_raises_after_max_attempts_on_generic_error(
        self, db_instance, monkeypatch
    ):
        def mock_connect(*args, **kwargs):
            raise pyodbc.Error("Generic error")

        db_instance.connection = None
        db_instance.connection_attempts = 3
        with patch("pyodbc.connect", side_effect=mock_connect):
            with pytest.raises(
                pyodbc.Error, match="Failed to connect to the database"
            ):
                db_instance._Db__connect()

    def test_connect_raises_error_when_connection_is_none(
        self, db_instance, monkeypatch
    ):

        def mock_connect(*args, **kwargs):
            return None

        monkeypatch.setattr("pyodbc.connect", mock_connect)

        db_instance.connection_attempts = 3
        db_instance.connection = None

        with pytest.raises(
            pyodbc.Error, match="Failed to connect to the database"
        ):
            db_instance._Db__connect()

    def test_are_connection_attempts_reached(self, db_instance, caplog):
        assert not db_instance._Db__are_connection_attempts_reached(1)
        assert "Retrying connection..." in caplog.text

        assert db_instance._Db__are_connection_attempts_reached(3)
        assert (
            "Failed to connect to the DataWarehouse after 3 attempts."
            in caplog.text
        )

    def test_disconnect(self, db_instance):
        mock_connection = Mock()
        db_instance.connection = mock_connection
        db_instance.cursor = Mock()

        db_instance._Db__disconnect()

        assert mock_connection.close.called
        assert db_instance.connection is None
        assert db_instance.cursor is None

    def test_disconnect_without_connection(self, db_instance):
        db_instance.connection = None
        db_instance.cursor = None

        db_instance._Db__disconnect()

    def test_commit(self, db_instance):
        mock_connection = Mock()
        db_instance.connection = mock_connection
        db_instance._Db__commit()
        mock_connection.commit.assert_called_once()
