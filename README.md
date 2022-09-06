# pyPrediktorMapClient Library

This library contains two python scripts to make direct APIS requests to download data. These two scripts define the functions that make API call in exact same way it is happening on web servers such as on Swagger for OPC UA. 

Using model index script, required structure data can be downloaded and further that can be used to make OPC UA API requests. 

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
