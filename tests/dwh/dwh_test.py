import pytest
import unittest
import pyodbc
import logging
import datetime
from unittest.mock import Mock, patch, MagicMock, call
from pyprediktormapclient.dwh.dwh import DWH


class TestCaseDWH:

    @pytest.fixture
    def mock_pyodbc_connect(self):
        with patch('pyodbc.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            yield mock_connect

    @pytest.fixture
    def dwh_instance(self, mock_pyodbc_connect):
        with patch.object(DWH, '_DWH__initialize_context_services'):
            return DWH("test_url", "test_db", "test_user", "test_pass", -1)
        
    @pytest.fixture
    def mock_iter_modules(self):
        with patch('pkgutil.iter_modules') as mock_iter_modules:
            yield mock_iter_modules

    @patch.object(DWH, '_DWH__initialize_context_services')
    @patch('pyprediktormapclient.dwh.dwh.Db.__init__')
    def test_init(self, mock_db_init, mock_initialize_context_services, mock_pyodbc_connect):
        DWH("test_url", "test_db", "test_user", "test_pass", -1)
        mock_db_init.assert_called_once_with("test_url", "test_db", "test_user", "test_pass", -1)
        mock_initialize_context_services.assert_called_once()

    @patch('pyprediktorutilities.dwh.dwh.pyodbc.connect')
    def test_init_connection_error(self, mock_connect):
        mock_connect.side_effect = pyodbc.DataError("Error code", "Error message")
        with pytest.raises(pyodbc.DataError):
            DWH("test_url", "test_db", "test_user", "test_pass", 0)

    @patch('pyprediktorutilities.dwh.dwh.pyodbc.connect')
    def test_init_connection_retry(self, mock_connect, caplog):
        mock_connect.side_effect = [
            pyodbc.DatabaseError("Error code", "Temporary error message"),
            pyodbc.DatabaseError("Error code", "Temporary error message"),
            pyodbc.DatabaseError("Error code", "Permanent error message")
        ]
        with caplog.at_level(logging.ERROR):
            with pytest.raises(pyodbc.DatabaseError):
                DWH("test_url", "test_db", "test_user", "test_pass", 0)
        assert "Failed to connect to the DataWarehouse after 3 attempts." in caplog.text

    @patch('pyodbc.connect')
    @patch('pyodbc.drivers')
    def test_init_default_driver(self, mock_drivers, mock_connect):
        mock_drivers.return_value = ["Driver1", "Driver2"]
        mock_connect.return_value = Mock()
        dwh = DWH("test_url", "test_db", "test_user", "test_pass")
        assert dwh.driver == "Driver1"

    @patch.object(DWH, 'fetch')
    def test_version_with_results(self, mock_fetch, dwh_instance):
        expected_result = {
            "DWHVersion": "2.3.1",
            "UpdateDate": datetime.datetime(2023, 11, 14, 7, 5, 19, 830000),
            "Comment": "Updated Dwh from procs",
            "MajorVersionNo": 2,
            "MinorVersionNo": 3,
            "InterimVersionNo": 1,
        }
        mock_fetch.return_value = [expected_result]
        version = dwh_instance.version()
        assert version == expected_result
        mock_fetch.assert_called_once_with("SET NOCOUNT ON; EXEC [dbo].[GetVersion]")

    @patch.object(DWH, 'fetch')
    def test_version_without_results(self, mock_fetch, dwh_instance):
        mock_fetch.return_value = []
        version = dwh_instance.version()
        assert version == {}
        mock_fetch.assert_called_once_with("SET NOCOUNT ON; EXEC [dbo].[GetVersion]")


    @patch('pyprediktormapclient.dwh.dwh.importlib.import_module')
    def test_initialize_context_services(self, mock_import_module, mock_iter_modules, dwh_instance):
        mock_iter_modules.return_value = [
            (None, 'pyprediktormapclient.dwh.context.enercast', False),
            (None, 'pyprediktormapclient.dwh.context.plant', False),
            (None, 'pyprediktormapclient.dwh.context.solcast', False)
        ]
        
        class TestService:
            def __init__(self, dwh):
                self.dwh = dwh

        def mock_import(name):
            mock_module = MagicMock()
            mock_module.__dir__ = lambda *args: ['TestService']
            mock_module.TestService = TestService
            return mock_module

        mock_import_module.side_effect = mock_import

        with patch.object(dwh_instance, '_is_attr_valid_service_class', return_value=True):
            dwh_instance._DWH__initialize_context_services()

        expected_calls = [
            call('pyprediktormapclient.dwh.context.enercast'),
            call('pyprediktormapclient.dwh.context.plant'),
            call('pyprediktormapclient.dwh.context.solcast')
        ]
        assert all(expected_call in mock_import_module.call_args_list for expected_call in expected_calls), \
            "Not all expected module imports were made"
        assert hasattr(dwh_instance, 'enercast')
        assert hasattr(dwh_instance, 'plant')
        assert hasattr(dwh_instance, 'solcast')
        
        for attr in ['enercast', 'plant', 'solcast']:
            assert isinstance(getattr(dwh_instance, attr), TestService)
            assert getattr(dwh_instance, attr).dwh == dwh_instance

    
    @patch('pyprediktormapclient.dwh.dwh.importlib.import_module')
    def test_initialize_context_services_with_modules(self, mock_import_module, mock_iter_modules, dwh_instance):
        mock_iter_modules.return_value = [
            (None, 'pyprediktormapclient.dwh.context.enercast', False),
            (None, 'pyprediktormapclient.dwh.context.plant', False),
            (None, 'pyprediktormapclient.dwh.context.solcast', False),
        ]

        class TestService:
            def __init__(self, dwh):
                self.dwh = dwh

        def mock_import(name):
            mock_module = MagicMock()
            mock_module.__dir__ = lambda *args: ['TestService']
            mock_module.TestService = TestService
            return mock_module

        mock_import_module.side_effect = mock_import
        dwh_instance._DWH__initialize_context_services()

        expected_modules = ['pyprediktormapclient.dwh.context.enercast', 
                            'pyprediktormapclient.dwh.context.plant', 
                            'pyprediktormapclient.dwh.context.solcast']
        imported_modules = [call[0][0] for call in mock_import_module.call_args_list]
        
        assert all(module in imported_modules for module in expected_modules), "Not all expected modules were imported"
        assert hasattr(dwh_instance, 'enercast'), "enercast attribute is missing"
        assert hasattr(dwh_instance, 'plant'), "plant attribute is missing"
        assert hasattr(dwh_instance, 'solcast'), "solcast attribute is missing"

        for attr in ['enercast', 'plant', 'solcast']:
            assert isinstance(getattr(dwh_instance, attr), TestService), f"{attr} is not an instance of TestService"
            assert getattr(dwh_instance, attr).dwh == dwh_instance, f"{attr}'s dwh is not the dwh_instance"

    
    @patch('pyprediktormapclient.dwh.dwh.importlib.import_module')
    def test_initialize_context_services_with_package(self, mock_import_module, mock_iter_modules, dwh_instance):
        mock_iter_modules.return_value = [
            (None, 'pyprediktormapclient.dwh.context.package', True),
        ]

        dwh_instance._DWH__initialize_context_services()

        imported_modules = [call[0][0] for call in mock_import_module.call_args_list]
        assert 'pyprediktormapclient.dwh.context.package' not in imported_modules, "Package was unexpectedly imported"
        assert not hasattr(dwh_instance, 'package'), "package attribute unexpectedly exists"

    def test_is_attr_valid_service_class(self, dwh_instance):
        class TestClass:
            pass
        
        class IDWH:
            pass

        assert dwh_instance._is_attr_valid_service_class(TestClass) is True
        assert dwh_instance._is_attr_valid_service_class(IDWH) is True  
        assert dwh_instance._is_attr_valid_service_class('not_a_class') is False

if __name__ == '__main__':
    unittest.main()
