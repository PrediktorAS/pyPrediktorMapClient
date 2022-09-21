import json
import pandas as pd
import numpy as np
import itertools
from pathlib import Path
import asyncio
from typing import Dict, List
from aiohttp import ClientSession
import logging
from pydantic import BaseModel, HttpUrl, AnyUrl
from pyprediktormapclient.shared import request_from_api


logger = logging.getLogger()


class OPCUrls(BaseModel):
    rest_url: HttpUrl
    opcua_url: AnyUrl


class OPC_UA:
    """Value data from the opc ua api server

    Returns:
        live and aggregated historical value data
    """

    def __init__(self, rest_url: str, opcua_url: str):
        """Class initializer

        Args:
            rest_url (str): The complete url of the OPC UA Values REST API. E.g. "http://127.0.0.1:13371/"
            opcua_url (str): The complete url of the OPC UA Server that is passed on to the REST server. E.g. "opc.tcp://127.0.0.1:4872"
        Returns:
            Object: The initialized class object
        """
        OPCUrls(rest_url=rest_url, opcua_url=opcua_url)
        self.rest_url = rest_url
        self.opcua_url = opcua_url

    def get_vars_node_ids(self, obj_dataframe: pd.DataFrame) -> List:
        """Function to get variables node ids of the objects

        Args:
            obj_dataframe (pd.DataFrame): object dataframe
        Returns:
            List: list of variables' node ids
        """
        objects_vars = obj_dataframe.get("Vars")
        if objects_vars is None:
            return None

        # Flatten the list
        vars_list = [x for xs in objects_vars for x in xs]
        vars_node_ids = [x["Id"] for x in vars_list]
        return vars_node_ids

    def expand_props_vars(self, json_df: pd.DataFrame):
        """Get a dataframe with required columns by expanding and merging back Vars and Props columns

        Args:
            json_df (pd.DataFrame): json dataframe of API request
        Returns:
            dataframe: Pandas dataframe
        """
        # Make list of column names except vars and props
        non_vars_props_columns = [
            x for x in json_df.columns if x not in ["Vars", "Props"]
        ]
        json_df1 = json_df.explode("Props").reset_index(drop=True)
        json_df1[["Parameter", "Value"]] = json_df1["Props"].apply(pd.Series)
        json_df1 = json_df1.drop(columns=["Props", "Vars"])
        # Create Pivot to convert parameter column into dataframe's columns
        json_df_props = json_df1.pivot(
            index=non_vars_props_columns, columns="Parameter", values="Value"
        ).reset_index()
        json_df_props.columns.name = None
        json_df_vars = json_df.explode("Vars").reset_index(drop=True)
        json_df_vars[["Variable", "VariableId"]] = json_df_vars["Vars"].apply(pd.Series)
        json_df_vars = json_df_vars.drop(columns=["Vars", "Props"])
        # Merge props and vars dataframes
        json_df_merged = json_df_vars.merge(json_df_props, on=non_vars_props_columns)
        return json_df_merged

    def split_node_id(self, node_id: str):
        """Functions to get node id(s) with namespace index and data format of the node

        Args:
            node_id (str): node id of a node
        Returns:
            Dict: dictionary with three elements
        """
        id_split = node_id.split(":")
        node_id_dict = {
            "Id": id_split[2],
            "Namespace": int(id_split[0]),
            "IdType": int(id_split[1]),
        }
        return node_id_dict

    def get_live_values_data(
        self, include_variables: List, obj_dataframe: pd.DataFrame
    ):
        """Request to get real time data values of the variables for the requested node(s)

        Args:
            include_variables (List): list of variables
            obj_dataframe (pd.DataFrame): dataframe of object ids
        """
        node_ids = self.get_vars_node_ids(obj_dataframe)
        var_node_ids = [x for x in node_ids if (x.split(".")[-1]) in include_variables]
        node_ids_dicts = [self.split_node_id(x) for x in var_node_ids]
        body = json.dumps(
            [
                {
                    "Connection": {"Url": self.opcua_url, "AuthenticationType": 1},
                    "NodeIds": node_ids_dicts,
                }
            ]
        )
        headers = {"Content-Type": "application/json"}
        from_api = request_from_api(self.rest_url, "POST", "values/get", body, headers)
        if from_api is None:
            return None

        response = pd.DataFrame(from_api)
        result = pd.json_normalize(response["Values"][0])
        if len(result.columns) == 6:
            result1 = result.drop(columns=["Value.Type", "ServerTimestamp"]).set_axis(
                ["Timestamp", "Value", "Code", "Quality"], axis=1
            )
        elif len(result.columns) == 4:
            result1 = result.drop(columns=["Value.Type", "ServerTimestamp"]).set_axis(
                ["Timestamp", "Value"], axis=1
            )
        df = self.expand_props_vars(obj_dataframe)
        name_column = [
            x
            for x in df
            if x in ["Name", "DisplayName", "DescendantName", "AncestorName"]
        ][0]
        df1 = df[["VariableId", name_column, "Variable"]].set_axis(
            ["Id", "Name", "Variable"], axis=1
        )
        # Filtering dataframe for the variables
        obj_dataframe1 = df1[df1["Variable"].isin(include_variables)].reset_index(
            drop=True
        )
        final_df = pd.concat([obj_dataframe1, result1], axis=1)
        return final_df

    def create_readvalueids_dict(self, node_id: str, agg_name: str) -> Dict:
        """A function to get ReadValueIds

        Args:
            node_id (str): node id of a node
            agg_name (str): name of aggregation
        Returns:
            Dict: dict. of node id with aggregation
        """
        id_split = node_id.split(":")
        read_value_id_dict = {
            "NodeId": {
                "Id": id_split[2],
                "Namespace": int(id_split[0]),
                "IdType": int(id_split[1]),
            },
            "AggregateName": agg_name,
        }
        return read_value_id_dict

    def chunk_datetimes(self, start_time, end_time, n_time_splits):
        """Function to get chunked datetimes for the selected time periods(dates)

        Args:
            start_time (_type_): time from
            end_time (_type_): time to
            n_time_splits (_type_): number of splits
        Returns:
            List: list of datetimes tuples
        """
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
        diff = (end_time - start_time) / n_time_splits
        date_list = []
        for idx in range(n_time_splits):
            date_list += [(start_time + diff * idx).strftime("%Y-%m-%dT%H:%M:%S.%fZ")]
        date_list += [end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")]
        start_end_list = list(zip(date_list[:-1], date_list[1:]))
        return start_end_list

    def chunk_ids(self, ids_list, n):
        """Yield successive n-sized chunks from ids_list."""
        for i in range(0, len(ids_list), n):
            yield ids_list[i : i + n]

    ############### Functions for multithreading API calls for aggregated historical data
    async def http_get_with_aiohttp(
        self, session: ClientSession, endpoint: str, data, timeout: int = 10e25
    ):
        """Request function for aiohttp based API request

        Args:
            session (ClientSession): clientSession of a period of time for the reqested data
            endpoint (str): endpoint of requested API call
            data (_type_): body data
            timeout (int, optional): time of one session. Defaults to 10E25.
        """
        headers = {"Content-Type": "application/json"}
        try:
            response = await session.post(
                url=self.rest_url + endpoint,
                data=data,
                headers=headers,
                timeout=timeout,
            )
        except:
            logger.error("Request Failed for this data :", json.dumps(data))

        filtered_response_json = None
        try:
            response_json = await response.json()
            filtered_response_json = self.filter_json_response(response_json)
        except json.decoder.JSONDecodeError as e:
            logger.exception("JSON Decoding Error")

        return filtered_response_json

    def filter_json_response(self, response_json: json):
        """Function to filter json data after requesting to opc ua server

        Args:
            response_json (json): api call response
        Returns:
            filtered json response
        """
        # Removing ServerNamespaces and Success from response_json
        response_json.pop("ServerNamespaces", None)
        response_json.pop("Success", None)
        # Selecting only HistoryReadResults
        history_read_results = response_json.get("HistoryReadResults", [])
        response_json["HistoryReadResults"] = [
            self.filter_read_results(x) for x in history_read_results
        ]
        return response_json

    def filter_read_results(self, x: Dict) -> Dict:
        """Function to filter HistoryReadResults' list of dictionaries

        Args:
            x (Dict): dict of json response
        Returns:
            Dict: filtered dict
        """
        y = {}
        y["NodeId"] = x["NodeId"]["Id"]
        xvalues = x["DataValues"]
        for v in xvalues:
            v["Value"] = v["Value"]["Body"]
        y["DataValues"] = xvalues
        return y

    def process_response_dataframe(self, df_result: pd.DataFrame):
        """This is a function to get historical aggregated data into require dataframe format

        Args:
            df_result (pd.DataFrame): api call's response for agg historical data request
        """
        df_result["Variable"] = df_result["NodeId"].str.split(".").str[-1]
        df_result1 = df_result.explode("DataValues").reset_index(drop=True)
        df_result2 = pd.json_normalize(df_result1.DataValues)
        df_merge = pd.concat([df_result1, df_result2], axis=1)
        df = df_merge.drop(columns=["DataValues"]).set_axis(
            ["Id", "Variable", "Value", "Timestamp", "Code", "Quality"], axis=1
        )
        return df

    async def get_agg_hist_value_data(
        self,
        start_time: str,
        end_time: str,
        pro_interval: int,
        agg_name: str,
        obj_dataframe: pd.DataFrame,
        include_variables: List,
        chunk_size: int = 100000,
        batch_size: int = 1000,
        max_workers: int = 50,
        timeout: int = 10e25,
    ):
        """Function to make aiohttp based multithreaded api requests to get aggregated historical data from opc ua api server and write the data in 'Data' folder in parquet files.

        Args:
            start_time (str): start time of requested data
            end_time (str): end time of requested data
            pro_interval (int): interval time of processing in milliseconds
            agg_name (str): name of aggregation
            obj_dataframe (pd.DataFrame): dataframe of object ids
            include_variables (List): list of variables to include
            chunk_size (int, optional): time chunk size. Defaults to 100000.
            batch_size (int, optional): size of each Id chunck. Defaults to 1000.
            max_workers (int, optional): maximum number of workers(CPU). Defaults to 50.
            timeout (int, optional): timeout time of one session. Defaults to 10E25.
            session (ClientSession, optional): session of one request. Default to None and is automatically created
        """
        node_ids = self.get_vars_node_ids(obj_dataframe)
        var_node_ids = [x for x in node_ids if (x.split(".")[-1]) in include_variables]
        read_value_ids = [
            self.create_readvalueids_dict(x, agg_name) for x in var_node_ids
        ]
        # Lenght of time series
        n_datapoints = (
            (pd.to_datetime(end_time) - pd.to_datetime(start_time)).total_seconds()
            * 1000
            / pro_interval
        )
        one_batch_datapoints = n_datapoints * batch_size
        # Number of required splits
        n_time_splits = int(np.ceil(one_batch_datapoints / chunk_size))
        # Node ids chunks
        id_chunk_list = list(self.chunk_ids(read_value_ids, batch_size))
        # Get datetime chunks
        start_end_list = self.chunk_datetimes(start_time, end_time, n_time_splits)
        body_elements = list(itertools.product(start_end_list, id_chunk_list))
        # Create body chunks
        body_list = []
        for x in body_elements:
            start_time_new = x[0][0]
            end_time_new = x[0][1]
            ids = x[1]
            body = json.dumps(
                {
                    "Connection": {"Url": self.opcua_url, "AuthenticationType": 1},
                    "StartTime": start_time_new,
                    "EndTime": end_time_new,
                    "ProcessingInterval": pro_interval,
                    "ReadValueIds": ids,
                }
            )
            body_list.append(body)
        endpoint = "values/historicalaggregated"
        # Chunk body
        one_time_body_count = max_workers
        body_chunks_list = list(self.chunk_ids(body_list, one_time_body_count))
        # Folder to save the downloaded data
        Path("Data/").mkdir(exist_ok=True)
        logger.info("No. of Body Chunks : " + str(len(body_chunks_list)))
        for j, body_chunks in enumerate(body_chunks_list):
            logger.info("Requesting data for body chunk no. : " + str(j))
            # Request chunkwise data
            async with ClientSession() as session:
                results = await asyncio.gather(
                    *[
                        self.http_get_with_aiohttp(session, endpoint, body, timeout)
                        for body in body_chunks
                    ]
                )
                # Create a dataframe and save as parquet
                results_list = []
                for res in results:
                    if res is not None:
                        for x in res["HistoryReadResults"]:
                            results_list.append(x)
                df_results = pd.DataFrame(results_list)
                df = self.process_response_dataframe(df_results)
                df.to_parquet("Data/data_chunk_" + str(j) + ".parquet")
        logger.info(" Data download is complete ")
