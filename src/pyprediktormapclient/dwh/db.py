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
        self,
        url: str,
        database: str,
        username: str,
        password: str,
        driver_index: int = -1,
    ) -> None:
        """Class initializer.

        Args:
            url (str): The URL of the sql server
            database (str): The name of the database
            username (str): The username
            password (str): The password
        """
        self.url = url
        self.cursor = None
        self.database = database
        self.username = username
        self.password = password
        self.connection = None

        self.__set_driver(driver_index)

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
            columns = [col[0] for col in self.cursor.description]
            data_set = [dict(zip(columns, row)) for row in self.cursor.fetchall()]

            if to_dataframe:
                data_sets.append(pd.DataFrame(data_set, columns=columns))
            else:
                data_sets.append(data_set)

            if not self.cursor.nextset():
                break

        return data_sets if len(data_sets) > 1 else data_sets[0]

    @validate_call
    def execute(self, query: str, *args, **kwargs) -> List[Any]:
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
            *args: Variable length argument list to pass to cursor.execute().
            **kwargs: Arbitrary keyword arguments to pass to cursor.execute().

        Returns:
            List[Any]: The results of the query.
        """
        self.__connect()
        try:
            self.cursor.execute(query, *args, **kwargs)
            result = self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            return []

        self.__commit()
        return result

    """
    Private - Driver
    """

    @validate_call
    def __set_driver(self, driver_index: int) -> None:
        """Sets the driver for the database connection.

        Args:
            driver (int): Index of the driver in the list of available drivers. If the index is -1 or
                in general below 0, pyPrediktorMapClient is going to choose
                the driver for you.
        
        Raises:
            ValueError: If no valid driver is found.
        """
        available_drivers = self.__get_list_of_available_and_supported_pyodbc_drivers()
    
        if not available_drivers:
            raise ValueError("No supported ODBC drivers found.")
        
        if driver_index < 0:
            self.driver = available_drivers[0]
        elif driver_index >= len(available_drivers):
            raise ValueError(
                f"Driver index {driver_index} is out of range. Please use "
                f"the __get_list_of_available_and_supported_pyodbc_drivers() method "
                f"to list all available drivers."
            )
        else:
            self.driver = available_drivers[driver_index]

    @validate_call
    def __get_list_of_supported_pyodbc_drivers(self) -> List[Any]:
        try:
            return pyodbc.drivers()
        except pyodbc.Error as err:
            logger.error(f"Error retrieving drivers: {err}")
            return []

    @validate_call
    def __get_list_of_available_and_supported_pyodbc_drivers(
        self
    ) -> List[Any]:
        available_drivers = []
        supported_drivers = self.__get_list_of_supported_pyodbc_drivers()

        for driver in supported_drivers:
            try:
                connection_string = (
                    f"UID={self.username};"
                    + f"PWD={self.password};"
                    + f"DRIVER={driver};"
                    + f"SERVER={self.url};"
                    + f"DATABASE={self.database};"
                )
                pyodbc.connect(connection_string, timeout=3)
                available_drivers.append(driver)
            except pyodbc.Error as err:
                logger.info(f"Driver {driver} could not connect: {err}")
                pass

        return available_drivers

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
                if self.connection:  # Ensure connection is valid before accessing cursor
                    self.cursor = self.connection.cursor()
                    logging.info(f"Connected to the database on attempt {attempt + 1}")
                    return
                else:
                    logging.info(f"Connection is None on attempt {attempt + 1}")
                    raise pyodbc.Error("Failed to connect to the database")

            # Exceptions once thrown there is no point attempting
            except pyodbc.ProgrammingError as err:
                logger.error(f"Programming Error {err.args[0] if err.args else 'No code'}: {err.args[1] if len(err.args) > 1 else 'No message'}")
                logger.warning(
                    "There seems to be a problem with your code. Please "
                    "check your code and try again."
                )
                raise

            except (pyodbc.DataError, pyodbc.IntegrityError, pyodbc.NotSupportedError) as err:
                logger.error(f"{type(err).__name__} {err.args[0] if err.args else 'No code'}: {err.args[1] if len(err.args) > 1 else 'No message'}")
                raise

            # Exceptions when thrown we can continue attempting
            except pyodbc.OperationalError as err:
                logger.error(f"Operational Error: {err.args[0] if err.args else 'No code'}: {err.args[1] if len(err.args) > 1 else 'No message'}")
                logger.warning(
                    "Pyodbc is having issues with the connection. This "
                    "could be due to the wrong driver being used. Please "
                    "check your driver with "
                    "the __get_list_of_available_and_supported_pyodbc_drivers() method "
                    "and try again."
                )
                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    break

            except (pyodbc.DatabaseError, pyodbc.Error) as err:
                logger.error(f"{type(err).__name__} {err.args[0] if err.args else 'No code'}: {err.args[1] if len(err.args) > 1 else 'No message'}")
                attempt += 1
                if self.__are_connection_attempts_reached(attempt):
                    break

        if not self.connection:
            raise pyodbc.Error("Failed to connect to the database")

    @validate_call
    def __are_connection_attempts_reached(self, attempt) -> bool:
        if attempt != self.connection_attempts:
            logger.warning("Retrying connection...")
            return False

        logger.error(
            f"Failed to connect to the DataWarehouse after "
            f"{self.connection_attempts} attempts."
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
