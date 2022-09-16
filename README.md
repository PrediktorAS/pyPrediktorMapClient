# pyPrediktorMapClient


    Helper functions for Prediktor Map Services


Helper functions for communicating with Prediktors OPC UA ModelIndex REST-API and OPC UA
Values REST-API. Typically used for data anlytics purposes, you'll find Jypiter Notebooks
with examples in the notebooks folder.

Install is primarily done through PyPi with `pip install pyPrediktorMapClient`. If you want to contribute or need
run the Jupyter Notebooks in the `notebooks` folder locally, please clone this repository.

## To use after install

Example:
```
from pyprediktormapclient.model_index import ModelIndex
from pyprediktormapclient.opc_ua import OPC_UA

model = ModelIndex(url=your_model_index_url)
tsdata = OPC_UA(rest_url=your_opcua_rest_url, opcua_url=your_opcua_server_uri)

obj_types = model.get_object_types()
```

Further information, documentation and module reference on [the documentation site](https://prediktoras.github.io/pyPrediktorMapClient) and check out the jypiter notebooks in the notebooks folder.

## Setup to Install
1. First clone the repository and navigate to the main folder of repository.
```
git clone git@github.com:PrediktorAS/pyPrediktorMapClient.git
```
2. Install dependencies
As this is a python package, dependencies are in setyp.py (actually in setup.cfg, as this is a pyScaffold project)
```
pip install -e .
```
