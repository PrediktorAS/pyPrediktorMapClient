import pyodbc
import logging
import pandas as pd
from typing import List, Any
from pydantic import validate_call

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Db:
    """Access a PowerView Data Warehouse or other SQL databases.

    Args:
        url (str): The URL of the sql server
        database (str): The name of the database
        username (str): The username
        password (str): The password

    Attributes:
        connection (pyodbc.Connection): The connection object
        cursor (pyodbc.Cursor): The cursor object
    """

    @validate_call
    def __init__(
        self, url: str, database: str, username: str, password: str, driver: int = 0
    ) -> None:
        """Class initializer.

        Args:
            url (str): The URL of the sql server
            database (str): The name of the database
            username (str): The username
            password (str): The password
        """
        self.__set_driver(driver)

        self.url = url
        self.cursor = None
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.connection_string = (
            f"UID={self.username};"
            + f"PWD={self.password};"
            + f"DRIVER={self.driver};"
            + f"SERVER={self.url};"
            + f"DATABASE={self.database};"
        )
        self.connection_attempts = 3

        self.__connect()

    def __enter__(self):
        return self

    @validate_call
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.__disconnect()

    """
    Public
    """

    @validate_call
    def fetch(self, query: str, to_dataframe: bool = False) -> List[Any]:
        """Execute the SQL query to get results from DWH and return the data.

        Use that method for getting data. That means that if you use SELECT or
        you'd like to call a stored procedure that returns one or more sets
        of data, that is the correct method to use.

        Use that method to GET.

        Args:
            query (str): The SQL query to execute.
            to_dataframe (bool): If True, return the results as a list
                of DataFrames.

        Returns:
            List[Any]: The results of the query. If DWH returns multiple
                data sets, this method is going to return a list
                of result sets (lists). If DWH returns a single data set,
                the method is going to return a list representing the single
                result set.

                If to_dataframe is True, the data inside each data set
                is going to be in DataFrame format.
        """
        self.__connect()
        self.cursor.execute(query)

        data_sets = []
        while True:
            data_set = []

            columns = [col[0] for col in self.cursor.description]
            for row in self.cursor.fetchall():
                data_set.append(
                    {name: row[index] for index, name in enumerate(columns)}
                )

            data_sets.append(pd.DataFrame(data_set) if to_dataframe else data_set)

            if not self.cursor.nextset():
                break

        return data_sets if len(data_sets) > 1 else data_sets[0]

    @validate_call
    def execute(self, query: str, commit: bool = True) -> List[Any]:
        """Execute the SQL query and return the results.

        For instance, if we create a new record in DWH by calling
        a stored procedure returning the id of the inserted element or
        in our query we use `SELECT SCOPE_IDENTITY() AS LastInsertedId;`,
        the DWH is going to return data after executing our write request.

        Please note that here we expect a single result set. Therefore DWH
        is obligated to return only one data set and also we're obligated to
        construct our query according to this requirement.

        Use that method to CREATE, UPDATE, DELETE or execute business logic.
        To NOT use for GET.

        Args:
            query (str): The SQL query to execute.
            commit (bool): If True, commit the changes to the database.

        Returns:
            List[Any]: The results of the query.
        """
        self.__connect()
        self.cursor.execute(query)

        result = self.cursor.fetchall()
        if commit:
            self.__commit()

        return result

    """
    Private - Driver
    """

    @validate_call
    def __set_driver(self, driver_index: int) -> None:
        """Sets the driver to use for the connection to the database.

        Args:
            driver (int): The index of the driver to use.
        """
        if self.__get_number_of_available_pyodbc_drivers() < (driver_index + 1):
            raise ValueError(
                f"Driver index {driver_index} is out of range. Please use "
                + f"the __get_list_of_available_pyodbc_drivers() method "
                + f"to list all available drivers."
            )

        self.driver = self.__get_list_of_available_pyodbc_drivers()[driver_index]

    @validate_call
    def __get_number_of_available_pyodbc_drivers(self) -> int:
        return len(self.__get_list_of_available_pyodbc_drivers())

    @validate_call
    def __get_list_of_available_pyodbc_drivers(self) -> List[Any]:
        return pyodbc.drivers()

    """
    Private - Connector & Disconnector
    """

    @validate_call
    def __connect(self) -> None:
        """Establishes a connection to the database."""
        if self.connection:
            return

        logging.info("Initiating connection to the database...")

        attempt = 0
        while attempt < self.connection_attempts:
            try:
                self.connection = pyodbc.connect(self.connection_string)
                self.cursor = self.connection.cursor()
                logging.info("Connection successfull!")
                break

            # Exceptions once thrown there is no point attempting
            except pyodbc.DataError as err:
                logger.error(f"Data Error {err.args[0]}: {err.args[1]}")
                raise
            except pyodbc.IntegrityError as err:
                logger.error(f"Integrity Error {err.args[0]}: {err.args[1]}")
                raise
            except pyodbc.ProgrammingError as err:
                logger.error(f"Programming Error {err.args[0]}: {err.args[1]}")
                logger.warning(
                    f"There seems to be a problem with your code. Please "
                    + f"check your code and try again."
                )
                raise
            except pyodbc.NotSupportedError as err:
                logger.error(f"Not supported {err.args[0]}: {err.args[1]}")
                raise

            # Exceptions when thrown we can continue attempting
            except pyodbc.OperationalError as err:
                logger.error(f"Operational Error {err.args[0]}: {err.args[1]}")
                logger.warning(
                    f"Pyodbc is having issues with the connection. This "
                    + f"could be due to the wrong driver being used. Please "
                    + f"check your driver with "
                    + f"the __get_list_of_available_pyodbc_drivers() method "
                    + f"and try again."
                )

                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    raise
            except pyodbc.DatabaseError as err:
                logger.error(f"Database Error {err.args[0]}: {err.args[1]}")

                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    raise
            except pyodbc.Error as err:
                logger.error(f"Generic Error {err.args[0]}: {err.args[1]}")

                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    raise

    @validate_call
    def __are_connection_attempts_reached(self, attempt) -> bool:
        if attempt != self.connection_attempts:
            logger.warning("Retrying connection...")
            return False

        logger.error(
            f"Failed to connect to the DataWarehouse after "
            + f"{self.connection_attempts} attempts."
        )
        return True

    @validate_call
    def __disconnect(self) -> None:
        """Closes the connection to the database."""
        if self.connection:
            self.connection.close()

            self.cursor = None
            self.connection = None

    """
    Private - Low level database operations
    """

    @validate_call
    def __commit(self) -> None:
        """Commits any changes to the database."""
        self.connection.commit()
