# Introduction


    Helper functions for Prediktor Map Services


Helper functions for communicating with Prediktors OPC UA ModelIndex REST-API
and OPC UA Values REST-API. Typically used for data anlytics purposes, you'll
find Jypiter Notebooks with examples in the notebooks folder.

Install is primarily done through PyPi with `pip install pyPrediktorMapClient`.
If you want to contribute or need run the Jupyter Notebooks in the `notebooks`
folder locally, please clone this repository.



# Development
If you'd like to contribute to pyPrediktorMapClient, please follow the steps below.

## Get the code
1. First clone the repository and navigate to the main folder of repository.
```
git clone git@github.com:PrediktorAS/pyPrediktorMapClient.git
```

## Setup
2. Create Virtual environment
```
python3 -m venv .venv
source .venv/bin/activate
```

3. Install dependencies
As this is a python package, dependencies are in setyp.py _(actually in setup.cfg,
as this is a pyScaffold project)_. Requirements.txt will perform the correct
installation and add a couple of additional packages.

```
pip install -r requirements.txt
```

## Run and build
4. Run tests
```
tox
```

5. Do your changes
Add your changes and create a new PR to be approved.

6. Build
```
tox -e build
```

## Changes
7. Please apply your changes. If they will facilitate the work of the person using pyPrediktorMapClient, especially the new features you've implemented, ensure that you describe your changes comprehensively and provide guidance in the README.md file under the chapter `Manual - How to Use` (check below).

8. Commit your changes to a new branch, push and create a new pull request for review.

## Publish
9. Publish to PyPi test and live
```
tox -e publish
tox -e publish -- --repository pypi
```

## Possible errors
When running `tox` command it may happen to face the following error (or similar):

```
ImportError while importing test module '/PATH_TO_PROJECT/pyPrediktorMapClient/tests/test_dwh.py'.
Hint: make sure your test modules/packages have valid Python names.
Traceback:
/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/importlib/__init__.py:126: in import_module
    return _bootstrap._gcd_import(name[level:], package, level)
tests/test_dwh.py:4: in <module>
    from pyprediktormapclient.dwh import DWH
src/pyprediktormapclient/dwh.py:1: in <module>
    import pyodbc
E   ImportError: dlopen(/PATH_TO_PROJECT/pyPrediktorMapClient/.tox/default/lib/python3.10/site-packages/pyodbc.cpython-310-darwin.so, 0x0002): symbol not found in flat namespace '_SQLAllocHandle'
```

A possible solution is to rebuild pyodbc via:
```
pip uninstall pyodbc
pip install --force-reinstall --no-binary :all: pyodbc
```

on Mac:
```
export LDFLAGS="-L/opt/homebrew/lib"
export CPPFLAGS="-I/opt/homebrew/include"
.tox/default/bin/pip install --force-reinstall --no-binary :all: pyodbc
```

The last commands adjust the environment variables to point to your Homebrew
installation paths and then rebuild pyodbc. In that way we make sure
the build environment knows where to find the unixODBC headers and libraries.
Also, make sure to run these commands within the activated tox environment.



# Manual - How to Use
## ModelIndex
Example:
```
from pyprediktormapclient.model_index import ModelIndex
from pyprediktormapclient.opc_ua import OPC_UA

model = ModelIndex(url=your_model_index_url)
tsdata = OPC_UA(rest_url=your_opcua_rest_url, opcua_url=your_opcua_server_uri)

obj_types = model.get_object_types()
```

## DWH
### Introduction
Helper functions to access a PowerView Data Warehouse or other SQL databases. This class is a wrapper around pyodbc and you can use all pyodbc methods as well as the provided methods. Look at the pyodbc documentation and use the cursor attribute to access the pyodbc cursor.

### Architecture of the DWH component
Here is a diagram that illustrates the architecture of the DWH component:
![My Image](./diagram-dwh.png)

### Low level usage
```
from pyprediktormapclient.dwh import DWH

dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")
results = dwh.fetch("SELECT * FROM mytable")
dwh.execute("INSERT INTO mytable VALUES (1, 'test')")
```

### High level usage
#### Database version
Get the database version:
```
from pyprediktormapclient.dwh import DWH

dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")
database_version = dwh.version()
```

#### Plant
```
from pyprediktormapclient.dwh import DWH

dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")

facility_name = 'JO-GL'
optimal_tracker_angles = dwh.plant.get_optimal_tracker_angles(facility_name)

facility_data = {'key': 'some data'}
dwh.plant.upsert_optimal_tracker_angles(facility_data)

plantname = 'PlantName'
ext_forecast_type_key = 1
data_type = 'SomeType'
has_thrown_error = False
message = "Optional message"
dwh.plant.insert_log(
    plantname,
    ext_forecast_type_key,
    data_type,
    has_thrown_error,
    message
)
```

#### Solcast
```
from pyprediktormapclient.dwh import DWH

dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")

plants_to_update = dwh.solcast.get_plants_to_update()

plantname = 'PlantName'
solcast_forecast_data = {'key': 'some data'}
dwh.solcast.upsert_forecast_data(plantname, solcast_forecast_data)
```

#### Enercast
```
from pyprediktormapclient.dwh import DWH

dwh = DWH("localhost", "mydatabase", "myusername", "mypassword")

enercast_plants = dwh.enercast.get_plants_to_update()

asset_name = 'some-asset-name'
live_meter_data = dwh.enercast.get_live_meter_data(asset_name)

enercast_forecast_data = {'key': 'some data'}
dwh.enercast.upsert_forecast_data(enercast_forecast_data)
```

## Want to know more?
Further information, documentation and module reference on
[the documentation site](https://prediktoras.github.io/pyPrediktorMapClient)
and check out the jypiter notebooks in the notebooks folder.