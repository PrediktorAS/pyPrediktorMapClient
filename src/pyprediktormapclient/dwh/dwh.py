import importlib
import logging
import pkgutil
from typing import Dict

from pydantic import validate_call
from pyprediktorutilities.dwh.dwh import Dwh as Db

from . import context
from .idwh import IDWH

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DWH(Db, IDWH):
    """Helper functions to access a PowerView Data Warehouse or other SQL
    databases. This class is a wrapper around pyodbc and you can use all pyodbc
    methods as well as the provided methods. Look at the pyodbc documentation
    and use the cursor attribute to access the pyodbc cursor.

    Args:
        url (str): The URL of the sql server
        database (str): The name of the database
        username (str): The username
        password (str): The password

    Attributes:
        connection (pyodbc.Connection): The connection object
        cursor (pyodbc.Cursor): The cursor object

    Examples - of low level usage:
        >>> from pyprediktormapclient.dwh import DWH
        >>>
        >>> dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")
        >>>
        >>> dwh.fetch("SELECT * FROM mytable")
        >>>
        >>> dwh.execute("INSERT INTO mytable VALUES (1, 'test')")

    Examples - of high level usage:
        >>> from pyprediktormapclient.dwh import DWH
        >>>
        >>> dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")
        >>>
        >>> database_version = dwh.version()
        >>>
        >>> enercast_plants = dwh.enercast.get_plants_to_update()
    """

    @validate_call
    def __init__(
        self,
        url: str,
        database: str,
        username: str,
        password: str,
        driver_index: int = -1,
    ) -> None:
        super().__init__(url, database, username, password, driver_index)
        self.__initialize_context_services()

    """
    Public
    """

    @validate_call
    def version(self) -> Dict:
        """Get the DWH version.

        Returns:
            Dict: A dictionary with the following keys (or similar): DWHVersion, UpdateDate, ImplementedDate, Comment, MajorVersionNo, MinorVersionNo, InterimVersionNo
        """
        query = "SET NOCOUNT ON; EXEC [dbo].[GetVersion]"
        results = self.fetch(query)
        return results[0] if len(results) > 0 else {}

    """
    Private
    """

    def __initialize_context_services(self) -> None:
        """Initialise all services defined in `context` folder.

        These are methods used to directly call certain stored
        procedures. For instance, class Enercast contains calls to
        stored procedures directly related to Enercast.
        """
        package = context
        prefix = package.__name__ + "."
        for _, modname, ispkg in pkgutil.iter_modules(
            package.__path__, prefix
        ):
            if not ispkg:
                module = importlib.import_module(modname)

                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)

                    if self._is_attr_valid_service_class(attribute):
                        service_name = modname.split(".")[-1]
                        setattr(self, service_name, attribute(self))

    def _is_attr_valid_service_class(self, attribute) -> bool:
        return isinstance(attribute, type) and attribute is not IDWH
