import asyncio
import copy
import json
import logging
from asyncio import Semaphore
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union

import aiohttp
import nest_asyncio
import pandas as pd
import requests
from aiohttp import ClientSession
from pydantic import AnyUrl, BaseModel
from pydantic_core import Url
from requests import HTTPError

from pyprediktormapclient.shared import request_from_api

nest_asyncio.apply()


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


class StatsCode(BaseModel):
    """Helper class to parse all values api's.

    Variables:
        Code: Optional[int] - Status code, described in https://reference.opcfoundation.org/v104/Core/docs/Part8/A.4.3/
        Symbol: Optional[str] - String value for status code, described in https://reference.opcfoundation.org/v104/Core/docs/Part8/A.4.3/
    """

    Code: Optional[int] = None
    Symbol: Optional[str] = None


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
    SourcePicoseconds: Optional[int] = None
    ServerTimestamp: Optional[datetime] = None
    ServerPicoseconds: Optional[int] = None
    StatusCode: Optional[StatsCode] = None


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

    SymbolCodes: List[StatsCode]


class WriteReturn(BaseModel):
    """Helper class to collect API output with API input to see successfull
    writes for nodes.

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


class AsyncIONotebookHelper:
    @staticmethod
    def run_coroutine(coroutine):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coroutine)


class Config:
    arbitrary_types_allowed = True


class OPC_UA:
    """Helper functions to access the OPC UA REST Values API server.

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

    def __init__(
        self,
        rest_url: AnyUrl,
        opcua_url: AnyUrl,
        namespaces: List = None,
        auth_client: object = None,
        session: requests.Session = None,
    ):
        """Class initializer.

        Args:
            rest_url (str): The complete url of the OPC UA Values REST API. E.g. "http://127.0.0.1:13371/"
            opcua_url (str): The complete url of the OPC UA Server that is passed on to the REST server. E.g. "opc.tcp://127.0.0.1:4872"
            namespaces (list): An optional but recommended ordered list of namespaces so that IDs match
        Returns:
            Object: The initialized class object
        """
        self.TYPE_DICT = {t["id"]: t["type"] for t in TYPE_LIST}
        self.rest_url = rest_url
        self.opcua_url = opcua_url
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
        }
        self.auth_client = auth_client
        self.session = session
        self.helper = AsyncIONotebookHelper()

        if not str(self.opcua_url).startswith("opc.tcp://"):
            raise ValueError("Invalid OPC UA URL")

        if self.auth_client is not None:
            if self.auth_client.token is not None:
                self.headers["Authorization"] = (
                    f"Bearer {self.auth_client.token.session_token}"
                )
        self.body = {
            "Connection": {"Url": self.opcua_url, "AuthenticationType": 1}
        }
        if namespaces:
            self.body["ClientNamespaces"] = namespaces

    def json_serial(self, obj):
        """JSON serializer for objects not serializable by default json
        code."""

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Url):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    def check_auth_client(self, content):
        if content.get("error").get("code") == 404:
            self.auth_client.request_new_ory_token()
            self.headers["Authorization"] = (
                f"Bearer {self.auth_client.token.session_token}"
            )
        else:
            raise RuntimeError(content.get("ErrorMessage"))

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

    def _get_variable_list_as_list(self, variable_list: list) -> list:
        """Internal function to convert a list of pydantic Variable models to a
        list of dicts.

        Args:
            variable_list (List): List of pydantic models

        Returns:
            List: List of dicts
        """
        new_vars = []
        for var in variable_list:
            if hasattr(var, "model_dump"):
                # Convert pydantic model to dict
                new_vars.append(var.model_dump())
            elif isinstance(var, dict):
                # If var is already a dict, append as is
                new_vars.append(var)
            else:
                raise TypeError("Unsupported type in variable_list")

        return new_vars

    def get_values(self, variable_list: List[Variables]) -> List:
        """Request realtime values from the OPC UA server.

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
            if self.auth_client is not None:
                self.check_auth_client(json.loads(e.response.content))
                content = request_from_api(
                    rest_url=self.rest_url,
                    method="POST",
                    endpoint="values/get",
                    data=json.dumps([body], default=self.json_serial),
                    headers=self.headers,
                    extended_timeout=True,
                )
            else:
                raise RuntimeError(f"Error in get_values: {str(e)}") from e
        except Exception as e:
            raise RuntimeError(f"Error in get_values: {str(e)}") from e

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
                vars[num]["StatusSymbol"] = contline["StatusCode"].get(
                    "Symbol"
                )

        return vars

    def _check_content(self, content: Dict[str, Any]) -> None:
        """Check the content returned from the server.

        Args:
            content (dict): The content returned from the server.

        Raises:
            RuntimeError: If the content is not a dictionary, if the request was not successful, or if the content does not contain 'HistoryReadResults'.
        """
        if not isinstance(content, dict):
            raise RuntimeError("No content returned from the server")
        if not content.get("Success"):
            raise RuntimeError(content.get("ErrorMessage"))
        if "HistoryReadResults" not in content:
            raise RuntimeError(
                "No history read results returned from the server"
            )

    def _process_df(
        self, df_result: pd.DataFrame, columns: Dict[str, str]
    ) -> pd.DataFrame:
        """Process the DataFrame returned from the server."""
        if "Value.Type" in df_result.columns:
            df_result["Value.Type"] = df_result["Value.Type"].replace(
                self.TYPE_DICT
            )

        df_result.rename(columns=columns, errors="raise", inplace=True)

        return df_result

    async def _make_request(
        self, endpoint: str, body: dict, max_retries: int, retry_delay: int
    ):
        for attempt in range(max_retries):
            try:
                logging.info(f"Attempt {attempt + 1} of {max_retries}")
                async with ClientSession() as session:
                    url = f"{self.rest_url}{endpoint}"
                    logging.info(f"Making POST request to {url}")
                    logging.debug(f"Request body: {body}")
                    logging.debug(f"Request headers: {self.headers}")

                    async with session.post(
                        url, json=body, headers=self.headers
                    ) as response:
                        logging.info(
                            f"Response received: Status {response.status}"
                        )

                        if response.status >= 400:
                            error_text = await response.text()
                            logging.error(
                                f"HTTP error {response.status}: {error_text}"
                            )
                            await response.raise_for_status()

                        return await response.json()

            except aiohttp.ClientResponseError as e:
                logging.error(f"ClientResponseError: {e}")
                if attempt == max_retries - 1:
                    raise RuntimeError("Max retries reached") from e
            except aiohttp.ClientError as e:
                logging.error(f"ClientError in POST request: {e}")
            except Exception as e:
                logging.error(f"Unexpected error in _make_request: {e}")

            if attempt < max_retries - 1:
                wait_time = retry_delay * (2**attempt)
                logging.warning(
                    f"Request failed. Retrying in {wait_time} seconds..."
                )
                await asyncio.sleep(wait_time)

        logging.error("Max retries reached.")
        raise RuntimeError("Max retries reached")

    def _process_content(self, content: dict) -> pd.DataFrame:
        self._check_content(content)
        df_list = []
        for item in content["HistoryReadResults"]:
            df = pd.json_normalize(item["DataValues"])
            for key, value in item["NodeId"].items():
                df[f"HistoryReadResults.NodeId.{key}"] = value
            df_list.append(df)

        if df_list:
            df_result = pd.concat(df_list)
            df_result.reset_index(inplace=True, drop=True)
            return df_result

    async def get_historical_values(
        self,
        start_time: datetime,
        end_time: datetime,
        variable_list: List[str],
        endpoint: str,
        prepare_variables: Callable[[List[str]], List[dict]],
        additional_params: dict = None,
        max_data_points: int = 10000,
        max_retries: int = 3,
        retry_delay: int = 5,
        max_concurrent_requests: int = 30,
    ) -> pd.DataFrame:
        """Generic method to request historical values from the OPC UA server
        with batching."""
        total_time_range_ms = (end_time - start_time).total_seconds() * 1000
        estimated_intervals = total_time_range_ms / max_data_points

        max_variables_per_batch = max(
            1, int(max_data_points / estimated_intervals)
        )
        max_time_batches = max(1, int(estimated_intervals / max_data_points))
        time_batch_size_ms = total_time_range_ms / max_time_batches

        extended_variables = prepare_variables(variable_list)
        variable_batches = [
            extended_variables[i : i + max_variables_per_batch]
            for i in range(0, len(extended_variables), max_variables_per_batch)
        ]

        semaphore = Semaphore(max_concurrent_requests)

        async def process_batch(variables, time_batch):
            async with semaphore:
                batch_start_ms = time_batch * time_batch_size_ms
                batch_end_ms = min(
                    (time_batch + 1) * time_batch_size_ms, total_time_range_ms
                )
                batch_start = start_time + timedelta(
                    milliseconds=batch_start_ms
                )
                batch_end = start_time + timedelta(milliseconds=batch_end_ms)

                body = {
                    **self.body,
                    "StartTime": batch_start.isoformat() + "Z",
                    "EndTime": batch_end.isoformat() + "Z",
                    "ReadValueIds": variables,
                    **(additional_params or {}),
                }

                content = await self._make_request(
                    endpoint, body, max_retries, retry_delay
                )
                return self._process_content(content)

        tasks = [
            process_batch(variables, time_batch)
            for variables in variable_batches
            for time_batch in range(max_time_batches)
        ]

        results = await asyncio.gather(*tasks)
        results = [df for df in results if df is not None]

        if not results:
            return pd.DataFrame()

        combined_df = pd.concat(results, ignore_index=True)
        return combined_df

    async def get_historical_raw_values_asyn(
        self,
        start_time: datetime,
        end_time: datetime,
        variable_list: List[str],
        limit_start_index: Union[int, None] = None,
        limit_num_records: Union[int, None] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Request raw historical values from the OPC UA server."""

        additional_params = {}
        if limit_start_index is not None and limit_num_records is not None:
            additional_params["Limit"] = {
                "StartIndex": limit_start_index,
                "NumRecords": limit_num_records,
            }

        combined_df = await self.get_historical_values(
            start_time,
            end_time,
            variable_list,
            "values/historical",
            lambda vars: [{"NodeId": var} for var in vars],
            additional_params,
            **kwargs,
        )
        columns = {
            "Value.Type": "ValueType",
            "Value.Body": "Value",
            "SourceTimestamp": "Timestamp",
            "HistoryReadResults.NodeId.IdType": "IdType",
            "HistoryReadResults.NodeId.Id": "Id",
            "HistoryReadResults.NodeId.Namespace": "Namespace",
        }
        return self._process_df(combined_df, columns)

    def get_historical_raw_values(self, *args, **kwargs):
        result = self.helper.run_coroutine(
            self.get_historical_raw_values_asyn(*args, **kwargs)
        )
        return result

    async def get_historical_aggregated_values_asyn(
        self,
        start_time: datetime,
        end_time: datetime,
        pro_interval: int,
        agg_name: str,
        variable_list: List[str],
        **kwargs,
    ) -> pd.DataFrame:
        """Request historical aggregated values from the OPC UA server."""

        additional_params = {
            "ProcessingInterval": pro_interval,
            "AggregateName": agg_name,
        }

        combined_df = await self.get_historical_values(
            start_time,
            end_time,
            variable_list,
            "values/historicalaggregated",
            lambda vars: [
                {"NodeId": var, "AggregateName": agg_name} for var in vars
            ],
            additional_params,
            **kwargs,
        )
        columns = {
            "Value.Type": "ValueType",
            "Value.Body": "Value",
            "StatusCode.Symbol": "StatusSymbol",
            "StatusCode.Code": "StatusCode",
            "SourceTimestamp": "Timestamp",
            "HistoryReadResults.NodeId.IdType": "IdType",
            "HistoryReadResults.NodeId.Id": "Id",
            "HistoryReadResults.NodeId.Namespace": "Namespace",
        }
        return self._process_df(combined_df, columns)

    def get_historical_aggregated_values(self, *args, **kwargs):
        result = self.helper.run_coroutine(
            self.get_historical_aggregated_values_asyn(*args, **kwargs)
        )
        return result

    def write_values(self, variable_list: List[WriteVariables]) -> List:
        """Request to write realtime values to the OPC UA server.

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
                content = request_from_api(
                    rest_url=self.rest_url,
                    method="POST",
                    endpoint="values/set",
                    data=json.dumps([body], default=self.json_serial),
                    headers=self.headers,
                    extended_timeout=True,
                )
            else:
                raise RuntimeError(f"Error in write_values: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Error in write_values: {str(e)}")

        # Return if no content from server
        if not isinstance(content, dict):
            return None
        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))
        if content.get("StatusCodes") is None:
            raise ValueError(
                "No status codes returned, might indicate no values written"
            )

        # Use to place successfull write next to each written values as API only returns list. Assumes same index in response as in request.
        for num, row in enumerate(vars):
            vars[num]["WriteSuccess"] = (
                lambda x: True if (x == 0) else False
            )(content["StatusCodes"][num].get("Code"))

        return vars

    def write_historical_values(
        self, variable_list: List[WriteHistoricalVariables]
    ) -> List:
        """Request to write realtime values to the OPC UA server.

        Args:
            variable_list (list): A list of variables you want, containing keys "Id", "Namespace", "Values" and "IdType". Values must be in descending order of the timestamps.
        Returns:
            list: The input variable_list extended with "Timestamp", "Value", "ValueType", "StatusCode" and "StatusSymbol" (all defaults to None)
        """
        # Check if data is in correct order, if wrong fail.
        for variable in variable_list:
            if len(variable.get("UpdateValues", [])) > 1:
                for num_variable in range(len(variable["UpdateValues"]) - 1):
                    if not (
                        (
                            variable["UpdateValues"][num_variable][
                                "SourceTimestamp"
                            ]
                        )
                        < variable["UpdateValues"][num_variable + 1][
                            "SourceTimestamp"
                        ]
                    ):
                        raise ValueError(
                            "Time for variables not in correct order."
                        )
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
                # Retry the request after checking auth
                content = request_from_api(
                    rest_url=self.rest_url,
                    method="POST",
                    endpoint="values/historicalwrite",
                    data=json.dumps(body, default=self.json_serial),
                    headers=self.headers,
                    extended_timeout=True,
                )
            else:
                raise RuntimeError(
                    f"Error in write_historical_values: {str(e)}"
                )
        except Exception as e:
            raise RuntimeError(f"Error in write_historical_values: {str(e)}")
        # Return if no content from server
        if not isinstance(content, dict):
            return None
        # Crash if success if false
        if content.get("Success") is False:
            raise RuntimeError(content.get("ErrorMessage"))
        # Crash if history report is missing
        if content.get("HistoryUpdateResults") is None:
            raise ValueError(
                "No status codes returned, might indicate no values written"
            )

        # Check if there are per history update error codes returned
        for num_var, variable_row in enumerate(vars):
            # Use to place successfull write next to each written values as API only returns list. Assumes same index in response as in request.
            if content["HistoryUpdateResults"][num_var] == {}:
                vars[num_var]["WriteSuccess"] = True
            else:
                vars[num_var]["WriteSuccess"] = False
                vars[num_var]["WriteError"] = content["HistoryUpdateResults"][
                    num_var
                ].get("StatusCode")

        return vars

    def check_if_ory_session_token_is_valid_refresh(self):
        """Check if the session token is still valid."""
        if self.auth_client.check_if_token_has_expired():
            self.auth_client.refresh_token()


TYPE_LIST = [
    {
        "id": 0,
        "type": "Null",
        "description": "An invalid or unspecified value",
    },
    {
        "id": 1,
        "type": "Boolean",
        "description": "A boolean logic value (true or false)",
    },
    {"id": 2, "type": "SByte", "description": "An 8 bit signed integer value"},
    {
        "id": 3,
        "type": "Byte",
        "description": "An 8 bit unsigned integer value",
    },
    {"id": 4, "type": "Int16", "description": "A 16 bit signed integer value"},
    {
        "id": 5,
        "type": "UInt16",
        "description": "A 16 bit unsigned integer value",
    },
    {"id": 6, "type": "Int32", "description": "A 32 bit signed integer value"},
    {
        "id": 7,
        "type": "UInt32",
        "description": "A 32 bit unsigned integer value",
    },
    {"id": 8, "type": "Int64", "description": "A 64 bit signed integer value"},
    {
        "id": 9,
        "type": "UInt64",
        "description": "A 64 bit unsigned integer value",
    },
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
    {
        "id": 12,
        "type": "String",
        "description": "A sequence of Unicode characters",
    },
    {"id": 13, "type": "DateTime", "description": "An instance in time"},
    {
        "id": 14,
        "type": "Guid",
        "description": "A 128-bit globally unique identifier",
    },
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
    {
        "id": 19,
        "type": "StatusCode",
        "description": "A structured result code",
    },
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
    {
        "id": 24,
        "type": "Variant",
        "description": "Any of the other built-in types",
    },
    {
        "id": 25,
        "type": "DiagnosticInfo",
        "description": "A diagnostic information associated with a result code",
    },
]
