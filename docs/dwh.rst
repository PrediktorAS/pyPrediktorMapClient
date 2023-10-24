.. _dwh:

=======
Working with the Data Warehouse
=======

The Data Warehouse in PowerView is a database that gathers data from various
sources and stores it in a format that is optimized for reporting.  Some of the
data within the Data Warehouse is updated on a regular basis, while other data
can be generated withing the Data Warehouse itself (such as KPIs).

The Data Warehouse is a Microsoft SQL database. To interface with the Data from
python, we use the `pyodbc`_ library.  This library allows us to connect to the
Data Warehouse and execute SQL queries, but this means that you can use the
functionality provided in this library to connect to any SQL database, e.g. the
"Model Results" database.

## pyodbc and drivers

The `pyodbc`_ library is a python library that allows you to connect to SQL
databases.  However, it does not provide the drivers that are needed to connect
to the databases.  The drivers are provided by the database vendors and must be
installed separately.  For example, to connect to the Data Warehouse, you need
to install the Microsoft SQL Server ODBC Driver. This driver is available for
download from the Microsoft website as descibed (here)[https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver16&tabs=macos].

When you initiate the `DWH` class, it will automatically try to connect using
the first driver that it finds.  If you have multiple drivers installed, you
can specify which driver to use by passing the `driver` argument to the `DWH`
as an input argument (integer). If you are not certain of the driver number,
you can use the `list_drivers` method to list all the drivers that are
available on your system, but only after you have initiated the `DWH` class.

## Connecting to the Data Warehouse

To connect to the Data Warehouse, you need to know the name of the server and
the database, as well as your username and password.  These are all provided
to you by the PowerView team.  Once you have this information, you can connect
by initating the `DWH` class like this:

.. code-block:: python

    from pyprediktormapclient.dwh import DWH
    dwh = DWH(server='server_name', database='database_name',
              username='username', password='password')

The `DWH` class has a `connect` method that will automatically connect to the
database when you initiate the class and the other functions within the class
will reconnect if the connection is lost. The `DWH` class also has a `disconnect`
method that will close the connection to the database, also automatically run with
the `__exit__` method.

The object returned by the `DWH` class is a `pyodbc.Connection` object, so you
can use any of the methods provided by that class.  For example, you can use
the `cursor` method to create a cursor object that can be used to execute SQL
or anythign else as (documented in the official project)[https://github.com/mkleehammer/pyodbc/wiki].

.. code-block:: python

    cursor = dwh.cursor()
    cursor.execute('SELECT * FROM dbo.DimDate')
    cursor.fetchall()


## Getting Data from the Data Warehouse

The `DWH` class has a number of methods that can be used to read and write data
from/to the Data Warehouse. These methods are documented in the API reference, but
here are some examples of how to use them.

### Reading Data

The `DWH` class has a `read` method that can be used to read data from the Data
Warehouse.  This method takes a SQL query as a string and returns the result as
a list with all the results. If you want to read the data into a pandas DataFrame,
you can use the `read_to_dataframe` method, which will return a pandas DataFrame.
Also, if you want to read the result line by line, you can use the call the cursor
method directly and iterate over the results with `fetchone` from `pyodbc`.

.. code-block:: python

    # Read data into a list
    data = dwh.read('SELECT * FROM dbo.DimDate')

    # Read data into a pandas DataFrame
    df = dwh.read_to_dataframe('SELECT * FROM dbo.DimDate')

    # Read data line by line
    cursor = dwh.cursor()
    cursor.execute('SELECT * FROM dbo.DimDate')
    while True:
        row = cursor.fetchone()
        if row == None:
            break
        print(row)

### Writing Data

The `DWH` class has a `write` method that can be used to write data to the Data
Warehouse.  This method takes a SQL query as a string and executes it.  If you
add the `commit=True` argument, the changes will be committed to the database
or rolled back if an error occurs. Alternatively, you can use the `commit` method
to commit the changes to the database. Failing to commit the changes will result
in the changes being lost.

.. code-block:: python

    # Write data to the database and commit the changes
    dwh.write('INSERT INTO dbo.DimDate VALUES (\'2018-01-01\')', commit=True)

    # or in two steps that would potentially allow you to read the data before
    # committing the changes
    dwh.write('INSERT INTO dbo.DimDate VALUES (\'2018-01-01\')')
    dwh.commit()
