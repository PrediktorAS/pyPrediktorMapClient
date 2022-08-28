# pyPrediktorMapClient Library

This library contains two python scripts to make direct APIS requests to download data. These two scripts define the functions that make API call in exact same way it is happening on web servers such as on Swagger for OPC UA. 

Using model index script, required structure data can be downloaded and further that can be used to make OPC UA API requests. 

## Setup to Install
1. First clone the repository and navigate to the main folder of repository.
```
git clone git@github.com:PrediktorAS/pyPrediktorMapClient.git
```
2. Make sure that you've [poetry](https://python-poetry.org/) installed.
Also change the following setting in `poetry` to create virtual environvent. Run the below command in terminal.
```
poetry config virtualenvs.in-project true
```
3. Open the repo in IDE (e.g. VS code) and run the following command in the terminal/commandline after navigating to the repo folder, this installs the dependencies defined in the `pyproject.toml` file.
```
poetry install
```