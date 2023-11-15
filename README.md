# Introduction


    Helper functions for Prediktor Map Services


Helper functions for communicating with Prediktors OPC UA ModelIndex REST-API
and OPC UA Values REST-API. Typically used for data anlytics purposes, you'll
find Jypiter Notebooks with examples in the notebooks folder.

Install is primarily done through PyPi with `pip install pyPrediktorMapClient`.
If you want to contribute or need run the Jupyter Notebooks in the `notebooks`
folder locally, please clone this repository.

# How to use?

Example:
```
from pyprediktormapclient.model_index import ModelIndex
from pyprediktormapclient.opc_ua import OPC_UA

model = ModelIndex(url=your_model_index_url)
tsdata = OPC_UA(rest_url=your_opcua_rest_url, opcua_url=your_opcua_server_uri)

obj_types = model.get_object_types()
```

Further information, documentation and module reference on
[the documentation site](https://prediktoras.github.io/pyPrediktorMapClient)
and check out the jypiter notebooks in the notebooks folder.

# Development
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

## Publish
7. Publish to PyPi test and live
```
tox -e publish
tox -e publish -- --repository pypi
```

# Possible errors
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