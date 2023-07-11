import json
import math
import logging
import datetime
import copy
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Union, Optional
from pydantic import BaseModel, HttpUrl, AnyUrl, validate_arguments
from pyprediktormapclient.shared import request_from_api
from requests import HTTPError
import asyncio
import multiprocessing
from functools import partial
import concurrent.futures
import os


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Variables(BaseModel):
    """Helper class to parse all values api's.
    Variables are described in https://reference.opcfoundation.org/v104/Core/docs/Part3/8.2.1/

        Variables:
            Id: str - Id of the signal, e.g. SSO.EG-AS.WeatherSymbol
            Namespace: int - Namespace on the signal, e.g. 2.
            IdType: int - IdTypes described in https://reference.opcfoundation.org/v104/Core/docs/Part3/8.2.3/.
    """
    Id: str
    Namespace: int
    IdType: int

class SubValue(BaseModel):
    """Helper class to parse all values api's.

        Variables:
            Type: int - Type of variable, e.g. 12. string, 11. float, etc. list of types described in https://reference.opcfoundation.org/Core/Part6/v104/5.1.2/ 
            Body: Union[str, float, int, bool] - The value of the varible, should match type.
    """
    Type: int
    Body: Union[str, float, int, bool]

class HistoryValue(BaseModel):
    """Helper class to parse all values api's.

        Variables:
            Value: SubValue - Containing Type and Body (value) of the variable. Described in SubValue class.
    """
    Value: SubValue

class StatusCode(BaseModel):
    """Helper class to parse all values api's.

        Variables:
            Code: Optional[int] - Status code, described in https://reference.opcfoundation.org/v104/Core/docs/Part8/A.4.3/
            Symbol: Optional[str] - String value for status code, described in https://reference.opcfoundation.org/v104/Core/docs/Part8/A.4.3/
    """
    Code: Optional[int]
    Symbol: Optional[str]

class Value(BaseModel):
    """Helper class to parse all values api's.

        Variables:
            Value: SubValue - Containing Type and Body (value) of the variable. Described in SubValue class.
            SourceTimestamp: str - Timestamp of the source, e.g. when coming from an API the timestamp returned from the API for the varaible is the sourcetimestamp.
            SourcePicoseconds: Optional[int] - Picoseconds for the timestamp of the source if there is a need for a finer granularity, e.g. if samples are sampled in picosecond level or more precision is needed.
            ServerTimestamp: Optional[str] - Timestamp for the server, normally this is assigned by the server.
            ServerPicoseconds: Optional[int] - Picoseconds for the timestamp on the server, normally this is assigned by the server.
            StatusCode: StatusCode - Status code, described in https://reference.opcfoundation.org/v104/Core/docs/Part8/A.4.3/
    """
    Value: SubValue
    SourceTimestamp: datetime
    SourcePicoseconds: Optional[int]
    ServerTimestamp: Optional[datetime]
    ServerPicoseconds: Optional[int]
    StatusCode: Optional[StatusCode]

class WriteVariables(BaseModel):
    """Helper class for write values api.

        Variables:
                NodeId: Variables - The complete node'id for the variable
                Value: Value - The value to update for the node'id.
    """
    NodeId: Variables
    Value: Value

class WriteHistoricalVariables(BaseModel):
    """Helper class for write historical values api.

        Variables:
            NodeId (str): The complete node'id for the variable
            PerformInsertReplace (int): Historical insertion method 1. Insert, 2. Replace 3. Update, 4. Remove
            UpdateValues (list): List of values to update for the node'id. Time must be in descending order.
    """
    NodeId: Variables
    PerformInsertReplace: int
    UpdateValues: List[Value]

class WriteVariablesResponse(BaseModel):
    """Helper class for write historical values api.

        Variables:
            SymbolCodes: List[StatusCode] - A list of class StatusCode, described in StatusCode class.
    """
    SymbolCodes: List[StatusCode]

class WriteReturn(BaseModel):
    """Helper class to collect API output with API input to see successfull writes for nodes.

        Variables:
            Id: str - The Id of the signal
            Value: str - The written value of the signal
            TimeStamp: str - THe SourceTimestamp of the written signal
            Success: bool - Success flag for the write operation
    """
    Id: str
    Value: str
    TimeStamp: str
    Success: bool

class OPC_UA:
    """Helper functions to access the OPC UA REST Values API server

    Args:
        rest_url (str): The complete url of the OPC UA Values REST API. E.g. "http://127.0.0.1:13371/"
        opcua_url (str): The complete url of the OPC UA Server that is passed on to the REST server. E.g. "opc.tcp://127.0.0.1:4872"
        namespaces (list): An optional but recommended ordered list of namespaces so that IDs match

    Returns:
        Object

    Todo:
        * Clean up use of files
        * Better session handling with aiohttp
        * Make sure that time convertions are with timezone
    """
    class Config:
        arbitrary_types_allowed = True


    @validate_arguments
    def __init__(self, rest_url: HttpUrl, opcua_url: AnyUrl, namespaces: List = None, auth_client: object = None):
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
        self.auth_client = auth_client
        if self.auth_client is not None:
            if self.auth_client.token is not None:
                self.headers["Authorization"] = f"Bearer {self.auth_client.token.access_token}"
        self.body = {"Connection": {"Url": self.opcua_url, "AuthenticationType": 1}}
        if namespaces:
            self.body["ClientNamespaces"] = namespaces

    def json_serial(self, obj):
        """JSON serializer for objects not serializable by default json code"""

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError (f"Type {type(obj)} not serializable")

    def check_auth_client(self, content):
        if (content.get('error').get('code') == 404):
            self.auth_client.request_new_ory_token()
            self.headers["Authorization"] = f"Bearer {self.auth_client.token.access_token}"
        else:
            raise RuntimeError(content.get("ErrorMessage"))

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

    def _get_variable_list_as_list(self, variable_list: List) -> List:
        """Internal function to convert a list of pydantic Variable models to a
        list of dicts

        Args:
            variable_list (List): List of pydantic models

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
        try:
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/get",
                data=json.dumps([body], default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )
        except HTTPError as e:
            # print(.get('error').get('code'))
            if self.auth_client is not None:
                self.check_auth_client(json.loads(e.response.content))
            else:
                raise RuntimeError(f'Error message {e}')
        finally:
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/get",
                data=json.dumps([body], default=self.json_serial),
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
        start_time: datetime,
        end_time: datetime,
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
        try:
            #Try making the request, if failes check if it is due to ory client
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/historicalaggregated",
                data=json.dumps(body, default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )
        except HTTPError as e:
            if self.auth_client is not None:
                self.check_auth_client(json.loads(e.response.content))
            else:
                raise RuntimeError(f'Error message {e}')
        finally:
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/historicalaggregated",
                data=json.dumps(body, default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )

        # Return if no content from server
        if not isinstance(content, dict):
            raise RuntimeError("No content returned from the server")

        # Return if not successful, but check ory status id ory is enabled
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

    ## New Functions

    @validate_arguments
    def get_historical_aggregated_values_batched_parallel(
            self,
        start_time: datetime,
        end_time: datetime,
        pro_interval: int,
        agg_name: str,
        variable_list: List[Variables],
        batch_size: int = 1000,
    ) -> pd.DataFrame:

        """Concurrent multithreading API requests to the OPC UA server for historical aggregated values"""

        # Configure the logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        # Set the number of CPU cores to utilize
        self.num_cores = os.cpu_count()

        
        time_batches = self.generate_time_batches(start_time, end_time, pro_interval, batch_size)
        logging.info("Number of time batches: %d", len(time_batches))
        variable_batches = self.generate_variable_batches(variable_list, batch_size)
        logging.info("Number of variable batches: %d", len(variable_batches))

        result_list = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_cores) as executor:
            futures = []

            for time_batch_start, time_batch_end in time_batches:
                for variable_sublist in variable_batches:
                    logging.info(f"Making API request for time batch: {time_batch_start} - {time_batch_end}")
                    future = executor.submit(
                        self.make_api_request,
                        time_batch_start,
                        time_batch_end,
                        pro_interval,
                        agg_name,
                        variable_sublist
                    )
                    futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                batch_response = future.result()
                logging.info("Processing API response...")
                batch_result = self.process_api_response(batch_response)
                result_list.append(batch_result)

        logging.info("Concatenating results...")
        result_df = pd.concat(result_list, ignore_index=True)
        return result_df

    @validate_arguments
    async def get_historical_aggregated_values_batched_async(
        self,
        start_time: datetime,
        end_time: datetime,
        pro_interval: int,
        agg_name: str,
        variable_list: List[Variables],
        batch_size: int = 1000
    ) -> pd.DataFrame:
        """Request historical aggregated values from the OPC UA server with batching"""

        # Configure the logging
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

        #logging.info("Generating time batches...")
        time_batches = self.generate_time_batches(start_time, end_time, pro_interval, batch_size)

        #logging.info("Generating variable batches...")
        variable_batches = self.generate_variable_batches(variable_list, batch_size)

        result_list = []

        for time_batch_start, time_batch_end in time_batches:
            for variable_sublist in variable_batches:
                logging.info(f"Making API request for time batch: {time_batch_start} - {time_batch_end}")
                batch_response = self.make_api_request(time_batch_start, time_batch_end, pro_interval, agg_name, variable_sublist)
                
                logging.info("Processing API response...")
                batch_result = self.process_api_response(batch_response)
                result_list.append(batch_result)

        logging.info("Concatenating results...")        
        result_df = pd.concat(result_list, ignore_index=True)

        return result_df

    @validate_arguments
    def get_historical_aggregated_values_batched(
        self,
        start_time: datetime,
        end_time: datetime,
        pro_interval: int,
        agg_name: str,
        variable_list: List[Variables],
        batch_size: int = 1000,
    ) -> pd.DataFrame:
        """Request historical aggregated values from the OPC UA server with batching"""


        time_batches = self.generate_time_batches(start_time, end_time, pro_interval, batch_size)
        variable_batches = self.generate_variable_batches(variable_list, batch_size)

        result_list = []

        for time_batch_start, time_batch_end in time_batches:
            for variable_sublist in variable_batches:
                batch_response = self.make_api_request(time_batch_start, time_batch_end, pro_interval, agg_name, variable_sublist)
                batch_result = self.process_api_response(batch_response)
                result_list.append(batch_result)
        result_df = pd.concat(result_list, ignore_index=True)

        return result_df

    def generate_time_batches(self, start_time: datetime, end_time: datetime, pro_interval: int, batch_size: int) -> List[tuple]:
        """Generate time batches based on start time, end time, processing interval, and batch size"""

        total_time_range = end_time - start_time
        pro_interval_seconds = (pro_interval / 1000)
        total_data_points = (total_time_range.total_seconds() // pro_interval_seconds) + 1

        total_batches = math.ceil(total_data_points / batch_size)
        actual_batch_size = math.ceil(total_data_points / total_batches)

        time_batches = [
            (start_time + timedelta(seconds=(i * actual_batch_size * pro_interval_seconds)),
            start_time + timedelta(seconds=((i + 1) * actual_batch_size * pro_interval_seconds)) - timedelta(seconds=pro_interval_seconds))
            for i in range(total_batches)
        ]

        return time_batches


    def generate_variable_batches(self, variable_list: List[Variables], batch_size: int) -> List[List[Variables]]:
        """Generate variable batches based on the variable list and batch size"""

        variable_batches = [
            variable_list[i:i + batch_size] for i in range(0, len(variable_list), batch_size)
        ]

        return variable_batches


    def make_api_request(self, start_time: datetime, end_time: datetime, pro_interval: int, agg_name: str, variable_list: List[Variables]) -> dict:
        """Make API request for the given time range and variable list"""

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

        try:
            # Make API request
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/historicalaggregated",
                data=json.dumps(body, default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )
        except HTTPError as e:
            if self.auth_client is not None:
                self.check_auth_client(json.loads(e.response.content))
            else:
                raise RuntimeError(f'Error message {e}')

        return content


    def process_api_response(self, response: dict) -> pd.DataFrame:
        """Process the API response and return the result dataframe"""

        # Return if no content from server
        if not isinstance(response, dict):
            raise RuntimeError("No content returned from the server")

        # Return if not successful, but check ory status id ory is enabled
        if response.get("Success") is False:
            raise RuntimeError(response.get("ErrorMessage"))

        # Check for HistoryReadResults
        if "HistoryReadResults" not in response:
            raise RuntimeError(response.get("ErrorMessage"))

        results_list = []
        for x in response["HistoryReadResults"]:
            results_list.append(x)

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
                df_result.at[i, "Type"] = self._get_value_type(int(row["Type"])).get("type")

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

    ## New Functions code ends here


    @validate_arguments
    def write_values(self, variable_list: List[WriteVariables]) -> List:
        """Request to write realtime values to the OPC UA server

        Args:
            variable_list (list): A list of variables you want to write to with the value, timestamp and quality, containing keys "Id", "Namespace", "Values" and "IdType".
        Returns:
            list: The input variable_list extended with "Timestamp", "Value", "ValueType", "StatusCode" and "StatusSymbol" (all defaults to None)
        """
        # Create a new variable list to remove pydantic models
        vars = self._get_variable_list_as_list(variable_list)
        body = copy.deepcopy(self.body)
        body["WriteValues"] = vars
        try:
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/set",
                data=json.dumps([body], default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )
        except HTTPError as e:
            if self.auth_client is not None:
                self.check_auth_client(json.loads(e.response.content))
            else:
                raise RuntimeError(f'Error message {e}')
        finally:
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/set",
                data=json.dumps([body], default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )
        # Return if no content from server
        if not isinstance(content, dict):
            return None
        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))
        if content.get("StatusCodes") is None:
            raise ValueError('No status codes returned, might indicate no values written')

        # Use to place successfull write next to each written values as API only returns list. Assumes same index in response as in request.
        for num, row in enumerate(vars):
            vars[num]["WriteSuccess"]=(lambda x : True if(x == 0) else False)(content['StatusCodes'][num].get("Code"))

        return vars

    @validate_arguments
    def write_historical_values(self, variable_list: List[WriteHistoricalVariables]) -> List:
        """Request to write realtime values to the OPC UA server

        Args:
            variable_list (list): A list of variables you want, containing keys "Id", "Namespace", "Values" and "IdType". Values must be in descending order of the timestamps.
        Returns:
            list: The input variable_list extended with "Timestamp", "Value", "ValueType", "StatusCode" and "StatusSymbol" (all defaults to None)
        """
        # Check if data is in correct order, if wrong fail.
        for variable in variable_list:
            if(len(variable.UpdateValues)>1):
                for num_variable in range(len(variable.UpdateValues) - 1):
                    if not((variable.UpdateValues[num_variable].SourceTimestamp) < variable.UpdateValues[num_variable+1].SourceTimestamp):
                        raise ValueError("Time for variables not in correct order.")
        # Create a new variable list to remove pydantic models
        vars = self._get_variable_list_as_list(variable_list)
        body = copy.deepcopy(self.body)
        body["UpdateDataDetails"] = vars
        try:
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/historicalwrite",
                data=json.dumps(body, default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )
        except HTTPError as e:
            if self.auth_client is not None:
                self.check_auth_client(json.loads(e.response.content))
            else:
                raise RuntimeError(f'Error message {e}')
        finally:
            content = request_from_api(
                rest_url=self.rest_url,
                method="POST",
                endpoint="values/historicalwrite",
                data=json.dumps(body, default=self.json_serial),
                headers=self.headers,
                extended_timeout=True,
            )
        # Return if no content from server
        if not isinstance(content, dict):
            return None
        # Crash if success if false
        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))
        # Crash if history report is missing
        if content.get("HistoryUpdateResults") is None:
            raise ValueError('No status codes returned, might indicate no values written')

        # Check if there are per history update error codes returned
        for num_var, variable_row in enumerate(vars):
            # Use to place successfull write next to each written values as API only returns list. Assumes same index in response as in request.
            if content["HistoryUpdateResults"][num_var] == {}:
                vars[num_var]["WriteSuccess"] = True
            else:
                vars[num_var]["WriteSuccess"] = False
                vars[num_var]["WriteError"] = content["HistoryUpdateResults"][num_var].get("StatusCode")

        return vars

    def check_if_ory_session_token_is_valid_refresh(self):
        """Check if the session token is still valid

        """
        if self.auth_client.check_if_token_has_expired():
            self.auth_client.refresh_token()


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
