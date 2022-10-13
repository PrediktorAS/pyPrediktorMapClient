import json
import pandas as pd
import numpy as np
import itertools
import math
from pathlib import Path
import asyncio
from typing import Dict, List
from aiohttp import ClientSession
import logging
from pydantic import BaseModel, HttpUrl, AnyUrl, validate_arguments
import datetime
import copy
from pyprediktormapclient.shared import request_from_api


logger = logging.getLogger()


class Variables(BaseModel):
    Id: str
    Namespace: int
    IdType: int


class OPC_UA:
    """Helper functions to access the OPC UA REST Values API server

    Args:
        rest_url (str): The complete url of the OPC UA Values REST API. E.g. "http://127.0.0.1:13371/"
        opcua_url (str): The complete url of the OPC UA Server that is passed on to the REST server. E.g. "opc.tcp://127.0.0.1:4872"
        namespaces (list): An optional but recommended ordered list of namespaces so that IDs match

    Returns:
        Object

    Todo:
        * Clean up logging
        * Use pydantic for argument validation
        * Remove all incoming datatable related actions
        * Clean up use of files
        * Better session handling on aiohttp
        * Make sure that time convertions are with timezone
    """

    @validate_arguments
    def __init__(self, rest_url: HttpUrl, opcua_url: AnyUrl, namespaces: List = None):
        """Class initializer

        Args:
            rest_url (str): The complete url of the OPC UA Values REST API. E.g. "http://127.0.0.1:13371/"
            opcua_url (str): The complete url of the OPC UA Server that is passed on to the REST server. E.g. "opc.tcp://127.0.0.1:4872"
            namespaces (list): An optional but recommended ordered list of namespaces so that IDs match
        Returns:
            Object: The initialized class object
        """
        self.rest_url = rest_url
        self.opcua_url = opcua_url
        self.headers = {"Content-Type": "application/json"}
        self.body = {"Connection": {"Url": self.opcua_url, "AuthenticationType": 1}}
        if namespaces:
            self.body["ClientNamespaces"] = namespaces

    @validate_arguments
    def _get_value_type(self, id: int) -> Dict:
        """Internal function to get the type of a value from the OPC UA return,as documentet at
        https://docs.prediktor.com/docs/opcuavaluesrestapi/datatypes.html#variant

        Args:
            id (int): An integer in the range 1-25 representing the id
        Returns:
            dict: Dictionaly with keys "id", "type" and "description". All with None values if not found
        """
        return next(
            (sub for sub in TYPE_LIST if sub["id"] == id),
            {"id": None, "type": None, "description": None},
        )

    @validate_arguments
    def _get_variable_list_as_list(self, variable_list: List[Variables]) -> List:
        """Internal function to convert a list of pydantic Variable models to a
        list of dicts

        Args:
            variable_list (List[Variables]): List of pydantic models adhering to Values class

        Returns:
            List: List of dicts
        """
        new_vars = []
        for var in variable_list:
            # Convert pydantic model to dict
            new_vars.append(var.dict())

        return new_vars

    @validate_arguments
    def get_values(self, variable_list: List[Variables]) -> List:
        """Request realtime values from the OPC UA server

        Args:
            variable_list (list): A list of variables you want, containing keys "Id", "Namespace" and "IdType"
        Returns:
            list: The input variable_list extended with "Timestamp", "Value", "ValueType", "StatusCode" and "StatusSymbol" (all defaults to None)
        """
        # Create a new variable list to remove pydantic models
        vars = self._get_variable_list_as_list(variable_list)
        body = copy.deepcopy(self.body)
        body["NodeIds"] = vars
        content = request_from_api(
            rest_url=self.rest_url,
            method="POST",
            endpoint="values/get",
            data=json.dumps([body]),
            headers=self.headers,
            extended_timeout=True,
        )

        for var in vars:
            # Add default None values
            var["Timestamp"] = None
            var["Value"] = None
            var["ValueType"] = None
            var["StatusCode"] = None
            var["StatusSymbol"] = None

        # Return if no content from server
        if not isinstance(content, list):
            return vars

        # Choose first item and return if not successful
        content = content[0]
        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))

        # Return if missing values
        if not content.get("Values"):
            return vars

        # Use .get from one dict to the other to ensure None values if something is missing
        for num, row in enumerate(vars):
            contline = content["Values"][num]
            vars[num]["Timestamp"] = contline.get("ServerTimestamp")
            # Values are not present in the answer if not found
            if "Value" in contline:
                vars[num]["Value"] = contline["Value"].get("Body")
                vars[num]["ValueType"] = self._get_value_type(
                    contline["Value"].get("Type")
                ).get("type")
            # StatusCode is not always present in the answer
            if "StatusCode" in contline:
                vars[num]["StatusCode"] = contline["StatusCode"].get("Code")
                vars[num]["StatusSymbol"] = contline["StatusCode"].get("Symbol")

        return vars

    @validate_arguments
    def get_historical_aggregated_values(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        pro_interval: int,
        agg_name: str,
        variable_list: List[Variables],
    ) -> pd.DataFrame:
        """Request historical aggregated values from the OPC UA server

        Args:
            start_time (datetime.datetime): start time of requested data
            end_time (datetime.datetime): end time of requested data
            pro_interval (int): interval time of processing in milliseconds
            agg_name (str): name of aggregation
            variable_list (list): A list of variables you want, containing keys "Id", "Namespace" and "IdType"
        Returns:
            pd.DataFrame: Columns in the DF are "StatusCode", "StatusSymbol", "ValueType", "Value", "Timestamp", "IdType", "Id", "Namespace"
        """
        # Create a new variable list to remove pydantic models
        vars = self._get_variable_list_as_list(variable_list)

        extended_variables = []
        for var in vars:
            extended_variables.append(
                {
                    "NodeId": var,
                    "AggregateName": agg_name,
                }
            )
        body = copy.deepcopy(self.body)
        body["StartTime"] = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        body["EndTime"] = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        body["ProcessingInterval"] = pro_interval
        body["ReadValueIds"] = extended_variables
        body["AggregateName"] = agg_name
        content = request_from_api(
            rest_url=self.rest_url,
            method="POST",
            endpoint="values/historicalaggregated",
            data=json.dumps(body),
            headers=self.headers,
            extended_timeout=True,
        )

        # Return if no content from server
        if not isinstance(content, dict):
            raise RuntimeError("No content returned from the server")

        # Return if not successful
        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))

        # Check for HistoryReadResults
        if not "HistoryReadResults" in content:
            raise RuntimeError(content.get("ErrorMessage"))

        results_list = []
        for x in content["HistoryReadResults"]:
            results_list.append(x)
        # print(results_list)
        df_result = pd.DataFrame(results_list)
        del df_result["StatusCode"]
        df_result = pd.concat(
            [df_result["NodeId"].apply(pd.Series), df_result.drop(["NodeId"], axis=1)],
            axis=1,
        )
        df_result = df_result.explode("DataValues").reset_index(drop=True)
        df_result = pd.concat(
            [
                df_result["DataValues"].apply(pd.Series),
                df_result.drop(["DataValues"], axis=1),
            ],
            axis=1,
        )
        df_result = pd.concat(
            [df_result["Value"].apply(pd.Series), df_result.drop(["Value"], axis=1)],
            axis=1,
        )
        df_result = pd.concat(
            [
                df_result["StatusCode"].apply(pd.Series),
                df_result.drop(["StatusCode"], axis=1),
            ],
            axis=1,
        )
        for i, row in df_result.iterrows():
            if not math.isnan(row["Type"]):
                df_result.at[i, "Type"] = self._get_value_type(int(row["Type"])).get(
                    "type"
                )

        df_result.rename(
            columns={
                "Type": "ValueType",
                "Body": "Value",
                "Symbol": "StatusSymbol",
                "Code": "StatusCode",
                "SourceTimestamp": "Timestamp",
            },
            errors="raise",
            inplace=True,
        )

        return df_result


TYPE_LIST = [
    {"id": 0, "type": "Null", "description": "An invalid or unspecified value"},
    {
        "id": 1,
        "type": "Boolean",
        "description": "A boolean logic value (true or false)",
    },
    {"id": 2, "type": "SByte", "description": "An 8 bit signed integer value"},
    {"id": 3, "type": "Byte", "description": "An 8 bit unsigned integer value"},
    {"id": 4, "type": "Int16", "description": "A 16 bit signed integer value"},
    {"id": 5, "type": "UInt16", "description": "A 16 bit unsigned integer value"},
    {"id": 6, "type": "Int32", "description": "A 32 bit signed integer value"},
    {"id": 7, "type": "UInt32", "description": "A 32 bit unsigned integer value"},
    {"id": 8, "type": "Int64", "description": "A 64 bit signed integer value"},
    {"id": 9, "type": "UInt64", "description": "A 64 bit unsigned integer value"},
    {
        "id": 10,
        "type": "Float",
        "description": "An IEEE single precision (32 bit) floating point value",
    },
    {
        "id": 11,
        "type": "Double",
        "description": "An IEEE double precision (64 bit) floating point value",
    },
    {"id": 12, "type": "String", "description": "A sequence of Unicode characters"},
    {"id": 13, "type": "DateTime", "description": "An instance in time"},
    {"id": 14, "type": "Guid", "description": "A 128-bit globally unique identifier"},
    {"id": 15, "type": "ByteString", "description": "A sequence of bytes"},
    {"id": 16, "type": "XmlElement", "description": "An XML element"},
    {
        "id": 17,
        "type": "NodeId",
        "description": "An identifier for a node in the address space of a UA server",
    },
    {
        "id": 18,
        "type": "ExpandedNodeId",
        "description": "A node id that stores the namespace URI instead of the namespace index",
    },
    {"id": 19, "type": "StatusCode", "description": "A structured result code"},
    {
        "id": 20,
        "type": "QualifiedName",
        "description": "A string qualified with a namespace",
    },
    {
        "id": 21,
        "type": "LocalizedText",
        "description": "A localized text string with an locale identifier",
    },
    {
        "id": 22,
        "type": "ExtensionObject",
        "description": "An opaque object with a syntax that may be unknown to the receiver",
    },
    {
        "id": 23,
        "type": "DataValue",
        "description": "A data value with an associated quality and timestamp",
    },
    {"id": 24, "type": "Variant", "description": "Any of the other built-in types"},
    {
        "id": 25,
        "type": "DiagnosticInfo",
        "description": "A diagnostic information associated with a result code",
    },
]
