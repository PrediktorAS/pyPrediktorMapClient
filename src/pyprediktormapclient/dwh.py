from pydantic import validate_call
import pyodbc
import pandas as pd
import logging
from typing import List, Any

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class DWH:
    """Helper functions to access a PowerView Data Warehouse or other SQL databases.
    This class is a wrapper around pyodbc and you can use all pyodbc methods as well
    as the provided methods. Look at the pyodbc documentation and use the cursor
    attribute to access the pyodbc cursor.

    Args:
        url (str): The URL of the sql server
        database (str): The name of the database
        username (str): The username
        password (str): The password
    
    Attributes:
        connection (pyodbc.Connection): The connection object
        cursor (pyodbc.Cursor): The cursor object

    Examples:
        >>> from pyprediktormapclient.dwh import DWH
        >>> dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")
        >>> dwh.read("SELECT * FROM mytable")
        >>> dwh.write("INSERT INTO mytable VALUES (1, 'test')")
        >>> dwh.commit() # Or commit=True in the write method
        >>> # You can also use the cursor directly
        >>> dwh.cursor.execute("SELECT * FROM mytable")
    """

    @validate_call
    def __init__(self, url: str, database: str, username: str, password: str, driver: int = 0) -> None:
        """Class initializer

        Args:
            url (str): The URL of the sql server
            database (str): The name of the database
            username (str): The username
            password (str): The password
        """
        self.url = url
        self.database = database
        self.username = username
        self.password = password
        self.driver = None
        self.__set_driver(driver)
        self.connectionstr = f"DRIVER={self.driver};SERVER={self.url};DATABASE={self.database};UID={self.username};PWD={self.password}"
        self.connection = None
        self.cursor = None
        
        self.connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.disconnect()

    def __set_driver(self, driver: int):
        """Sets the driver to use for the connection. Private function for internal use.

        Args:
            driver (int): The index of the driver to use.
        """
        driverlist = self.list_drivers()
        if len(driverlist) < (driver + 1):
            raise ValueError(f"Driver index {driver} is out of range. Please use the list_drivers() method to list all available drivers.")

        self.driver = pyodbc.drivers()[driver]
        
    def list_drivers(self):
        """Lists all available drivers for pyodbc."""
        return pyodbc.drivers()
        
    def connect(self):
        """Establishes a connection to the database."""
        if self.connection:
            return

        logging.info("Initiating connection to the database...")
        try:
            self.connection = pyodbc.connect(self.connectionstr)
            self.cursor = self.connection.cursor()
            logging.info("Connection successfull...")
        except pyodbc.OperationalError as err:
            logger.error(f"Operational Error {err.args[0]}: {err.args[1]}")
            logger.warning(f"Pyodbc is having issues with the connection. This could be due to the wrong driver being used. Please check your driver with the list_drivers() method and try again.")
            raise
        except pyodbc.DataError as err:
            logger.error(f"Data Error {err.args[0]}: {err.args[1]}")
            raise
        except pyodbc.IntegrityError as err:
            logger.error(f"Integrity Error {err.args[0]}: {err.args[1]}")
            raise
        except pyodbc.ProgrammingError as err:
            logger.error(f"Programming Error {err.args[0]}: {err.args[1]}")
            logger.warning(f"There seems to be a problem with your code. Please check your code and try again.")
            raise
        except pyodbc.NotSupportedError as err:
            logger.error(f"Not supported {err.args[0]}: {err.args[1]}")
            raise
        except pyodbc.DatabaseError as err:
            logger.error(f"Database Error {err.args[0]}: {err.args[1]}")
            raise
        except pyodbc.Error as err:
            logger.error(f"Generic Error {err.args[0]}: {err.args[1]}")
            raise
            
    def disconnect(self):
        """Closes the connection to the database."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None
            
    def read(self, sql: str) -> List[Any]:
        """Executes a SQL query and returns the results.
        
        Args:
            sql (str): The SQL query to execute.
            
        Returns:
            List[Any]: The results of the query.
        """
        self.connect()
        self.cursor.execute(sql)
        return self.cursor.fetchall()
        
    def write(self, sql: str, commit: bool = False) -> List[Any]:
        """Executes a SQL query and returns the results.
        
        Args:
            sql (str): The SQL query to execute.
            commit (bool): Whether to commit the changes to the database.
        
        Returns:
            List[Any]: The results of the query.
        """
        self.connect()
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if commit: self.commit()
        return result

    def executemany(self, sql: str, params: List[Any], commit: bool = False) -> List[Any]:
        """Executes a SQL query against all parameters or mappings and returns the results.
        
        Args:
            sql (str): The SQL query to execute.
            params (List[Any]): The parameters or mappings to use.
            commit (bool): Whether to commit the changes to the database.
        
        Returns:
            List[Any]: The results of the query.
        """
        self.connect()
        self.cursor.executemany(sql)
        result = self.cursor.fetchall()
        if commit: self.commit()
        return result
    
    def read_to_dataframe(self, sql: str) -> pd.DataFrame:
        """Executes a SQL query and returns the results as a DataFrame.
        
        Args:
            sql (str): The SQL query to execute.
            
        Returns:
            pd.DataFrame: The results of the query.
        
        """
        self.connect()
        return pd.read_sql(sql, self.connection)
    
    def commit(self):
        """Commits any changes to the database."""
        self.connection.commit()
        
        
