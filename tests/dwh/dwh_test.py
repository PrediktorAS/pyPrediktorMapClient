import datetime
import inspect
import unittest
from typing import Dict
from unittest.mock import MagicMock, call, patch

import pytest

from pyprediktormapclient.dwh.dwh import DWH
from pyprediktormapclient.dwh.idwh import IDWH


class TestCaseDWH:

    class InternalTestService:
        def __init__(self, dwh):
            self.dwh = dwh

    def test_init_default_driver(
        self, mock_pyodbc_drivers, mock_pyodbc_connect
    ):
        dwh = DWH("test_url", "test_db", "test_user", "test_pass")
        assert dwh.driver == "Driver1"

    def test_idwh_abstract_methods(self):
        assert inspect.isabstract(IDWH)
        assert set(IDWH.__abstractmethods__) == {"version", "fetch", "execute"}

    def test_dwh_implements_abstract_idwh(self, dwh_instance):
        def compare_signatures(impl_method, abstract_method):
            impl_sig = inspect.signature(impl_method)
            abstract_sig = inspect.signature(abstract_method)

            assert impl_sig.return_annotation == abstract_sig.return_annotation

            impl_params = list(impl_sig.parameters.values())[1:]
            abstract_params = list(abstract_sig.parameters.values())[1:]
            assert impl_params == abstract_params

        assert hasattr(dwh_instance, "version")
        assert callable(dwh_instance.version)
        compare_signatures(dwh_instance.version, IDWH.version)

        assert dwh_instance.version.__annotations__["return"] == Dict

    def test_idwh_instantiation_raises_error(self):
        with pytest.raises(TypeError) as excinfo:
            IDWH()
        error_message = str(excinfo.value)
        assert "Can't instantiate abstract class IDWH" in error_message
        assert "abstract methods" in error_message
        assert "execute" in error_message
        assert "fetch" in error_message
        assert "version" in error_message

    @patch.object(DWH, "fetch")
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
        mock_fetch.assert_called_once_with(
            "SET NOCOUNT ON; EXEC [dbo].[GetVersion]"
        )

    @patch.object(DWH, "fetch")
    def test_version_without_results(self, mock_fetch, dwh_instance):
        mock_fetch.return_value = []
        version = dwh_instance.version()
        assert version == {}
        mock_fetch.assert_called_once_with(
            "SET NOCOUNT ON; EXEC [dbo].[GetVersion]"
        )

    def setup_mock_imports(
        self, mock_iter_modules, mock_import_module, dwh_instance
    ):
        mock_iter_modules.return_value = [
            (None, "pyprediktormapclient.dwh.context.enercast", False),
            (None, "pyprediktormapclient.dwh.context.plant", False),
            (None, "pyprediktormapclient.dwh.context.solcast", False),
        ]

        def mock_import(name):
            mock_module = MagicMock()
            mock_module.__dir__ = lambda *args: ["InternalTestService"]
            mock_module.InternalTestService = self.InternalTestService
            return mock_module

        mock_import_module.side_effect = mock_import
        dwh_instance._DWH__initialize_context_services()

    @patch("pyprediktormapclient.dwh.dwh.importlib.import_module")
    def test_initialize_context_services(
        self, mock_import_module, mock_iter_modules, dwh_instance
    ):
        self.setup_mock_imports(
            mock_iter_modules, mock_import_module, dwh_instance
        )

        expected_calls = [
            call("pyprediktormapclient.dwh.context.enercast"),
            call("pyprediktormapclient.dwh.context.plant"),
            call("pyprediktormapclient.dwh.context.solcast"),
        ]
        assert all(
            expected_call in mock_import_module.call_args_list
            for expected_call in expected_calls
        ), "Not all expected module imports were made"
        assert hasattr(dwh_instance, "enercast")
        assert hasattr(dwh_instance, "plant")
        assert hasattr(dwh_instance, "solcast")

        for attr in ["enercast", "plant", "solcast"]:
            assert isinstance(
                getattr(dwh_instance, attr), self.InternalTestService
            )
            assert getattr(dwh_instance, attr).dwh == dwh_instance

    @patch("pyprediktormapclient.dwh.dwh.importlib.import_module")
    def test_initialize_context_services_with_modules(
        self, mock_import_module, mock_iter_modules, dwh_instance
    ):
        self.setup_mock_imports(
            mock_iter_modules, mock_import_module, dwh_instance
        )

        expected_modules = [
            "pyprediktormapclient.dwh.context.enercast",
            "pyprediktormapclient.dwh.context.plant",
            "pyprediktormapclient.dwh.context.solcast",
        ]
        imported_modules = [
            call[0][0] for call in mock_import_module.call_args_list
        ]

        assert all(
            module in imported_modules for module in expected_modules
        ), "Not all expected modules were imported"
        assert hasattr(
            dwh_instance, "enercast"
        ), "enercast attribute is missing"
        assert hasattr(dwh_instance, "plant"), "plant attribute is missing"
        assert hasattr(dwh_instance, "solcast"), "solcast attribute is missing"

        for attr in ["enercast", "plant", "solcast"]:
            assert isinstance(
                getattr(dwh_instance, attr), self.InternalTestService
            ), f"{attr} is not an instance of InternalTestService"
            assert (
                getattr(dwh_instance, attr).dwh == dwh_instance
            ), f"{attr}'s dwh is not the dwh_instance"

    @patch("pyprediktormapclient.dwh.dwh.importlib.import_module")
    def test_initialize_context_services_with_package(
        self, mock_import_module, mock_iter_modules, dwh_instance
    ):
        mock_iter_modules.return_value = [
            (None, "pyprediktormapclient.dwh.context.package", True),
        ]

        dwh_instance._DWH__initialize_context_services()

        imported_modules = [
            call[0][0] for call in mock_import_module.call_args_list
        ]
        assert (
            "pyprediktormapclient.dwh.context.package" not in imported_modules
        ), "Package was unexpectedly imported"
        assert not hasattr(
            dwh_instance, "package"
        ), "package attribute unexpectedly exists"

    def test_is_attr_valid_service_class(self, dwh_instance):
        class TestClass:
            pass

        class IDWH:
            pass

        assert dwh_instance._is_attr_valid_service_class(TestClass) is True
        assert dwh_instance._is_attr_valid_service_class(IDWH) is True
        assert (
            dwh_instance._is_attr_valid_service_class("not_a_class") is False
        )


if __name__ == "__main__":
    unittest.main()
