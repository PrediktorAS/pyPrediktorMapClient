from pydantic import validate_call
import pyodbc
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class DWH:
    """Helper functions to access a PowerView Data Warehouse

    Args:
        url (str): The URL of the sql server
        database (str): The name of the database
        username (str): The username
        password (str): The password
    """

    @validate_call
    def __init__(self, url: str, database: str, username: str, password: str, driver: int = 0) -> object:
        """Class initializer

        Args:
            url (str): The URL of the sql server
            database (str): The name of the database
            username (str): The username
            password (str): The password
        Returns:
            Object: The initialized class object
        """
        self.url = url
        self.database = database
        self.username = username
        self.password = password
        self.driver = pyodbc.drivers()[driver]
        self.connectionstr = f"DRIVER={self.driver};SERVER={self.url};DATABASE={self.database};UID={self.username};PWD={self.password}"
        self.connection = None
        self.cursor = None
        
        self.connect()

    def connect_status(self):
        if self.connection:
            return True
        else:
            return False
        
    def list_drivers(self):
        return pyodbc.drivers()
        
    def connect(self):
        logging.info("Initiating connection to the database...")
        try:
            self.connection = pyodbc.connect(self.connectionstr)
            self.cursor = self.connection.cursor()
            logging.info("Connection successfull...")
        except pyodbc.OperationalError as err:
            logger.error(f"Operational Error {err.args[0]}: {err.args[1]}")
            logger.warning(f"Pyodbc is having issues with the connection. This could be due to the wrong driver being used. Please check your driver with the list_drivers() method and try again.")
        except pyodbc.DataError as err:
            logger.error(f"Data Error {err.args[0]}: {err.args[1]}")
        except pyodbc.IntegrityError as err:
            logger.error(f"Integrity Error {err.args[0]}: {err.args[1]}")
        except pyodbc.ProgrammingError as err:
            logger.error(f"Programming Error {err.args[0]}: {err.args[1]}")
            logger.warning(f"There seems to be a problem with your code. Please check your code and try again.")
        except pyodbc.NotSupportedError as err:
            logger.error(f"Not supported {err.args[0]}: {err.args[1]}")
        except pyodbc.DatabaseError as err:
            logger.error(f"Database Error {err.args[0]}: {err.args[1]}")
        except pyodbc.Error as err:
            logger.error(f"Generic Error {err.args[0]}: {err.args[1]}")
            
    def disconnect(self):
        self.connection.close()
        
    def execute(self, sql: str):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def executemany(self, sql: str):
        self.cursor.executemany(sql)
        return self.cursor.fetchall()
    
    def read_to_dataframe(self, sql: str):
        return pd.read_sql(sql, self.connection)
    
    def commit(self):
        self.connection.commit()
        
        
