from ast import Str
import requests
import json
import pandas as pd
import numpy as np
import itertools
from pathlib import Path
from model_index import ModelIndex

import asyncio
import time
from typing import Dict, Any, List, Tuple
from itertools import repeat
from aiohttp import ClientSession
import logging


logger = logging.getLogger()

# Connection to the servers
model_index_url = "http://10.241.68.86:7001/v1/"
mdx = ModelIndex(url=model_index_url)

class OPC_UA:
    """Value data from the opc ua api server 

    Returns:
        live and aggregated historical value data
    """
    def __init__(self, url: str):
        self.url = url

    def request(self, method: str, endpoint: str, data=None,headers=None):       
        if method == 'GET':
            result = requests.get(self.url + endpoint)
        elif method == 'POST':
            result = requests.post(self.url + endpoint, data=data,headers=headers)
        else:
            raise Exception('Method not supported')
        if result.status_code == 200:
            return result.json()
        else:
            return None
 

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
            "IdType": int(id_split[1])
        }
        return node_id_dict

    def get_live_values_data(self, server_url: str, include_variables: List, obj_dataframe: pd.DataFrame):
        """Request to get real time data values of the variables for the requested node(s)

        Args:
            server_url (str): server connection url
            include_variables (List): list of variables 
            obj_dataframe (pd.DataFrame): dataframe of object ids
        """
        node_ids = mdx.get_vars_node_ids(obj_dataframe)
        var_node_ids = [x for x in node_ids if (x.split(".")[-1]) in include_variables]
        node_ids_dicts = [self.split_node_id(x) for x in var_node_ids]
        body = json.dumps([
            {
                "Connection": {
                    "Url": server_url,
                    "AuthenticationType": 1
                },
                "NodeIds": node_ids_dicts
            }
        ])
        headers = {'Content-Type': 'application/json'}
        response = pd.DataFrame(self.request('POST', 'values/get', body,headers))
        result = pd.json_normalize(response['Values'][0])
        result1 = result.drop(columns=['Value.Type','ServerTimestamp']).set_axis(['Timestamp', 'Value'], axis=1)
        df = mdx.expand_props_vars(obj_dataframe)
        name_column = [x for x in df if x in ['DisplayName','DescendantName', 'AncestorName']][0]
        df1 = df[['VariableId', name_column, 'Variable']].set_axis(['Id', 'Name', 'Variable'], axis=1)
        # Filtering dataframe for the variables
        obj_dataframe1 = df1[df1['Variable'].isin(include_variables)].reset_index(drop=True)
        final_df = pd.concat([obj_dataframe1,result1], axis=1)
        return final_df

    def create_readvalueids_dict(self, node_id: str, agg_name: str)-> Dict:
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
            "IdType": int(id_split[1])
        },
        "AggregateName": agg_name
        }
        return read_value_id_dict

    def chunk_datetimes(self,start_time, end_time, n_time_splits):
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
        diff = (end_time  - start_time) / n_time_splits
        date_list = []
        for idx in range(n_time_splits):
            date_list+=[(start_time + diff * idx).strftime("%Y-%m-%dT%H:%M:%S.%fZ")]
        date_list+=[end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")]
        start_end_list = list(zip(date_list[:-1], date_list[1:]))
        return start_end_list

    def chunk_ids(self, ids_list, n):
        """Yield successive n-sized chunks from ids_list."""
        for i in range(0, len(ids_list), n):
            yield ids_list[i:i + n]

    ############### Functions for multithreading API calls for aggregated historical data
    async def http_get_with_aiohttp(self,session: ClientSession, endpoint: str, data, timeout: int = 10E25):
        """Request function for aiohttp based API request

        Args:
            session (ClientSession): clientSession of a period of time for the reqested data
            endpoint (str): endpoint of requested API call
            data (_type_): body data
            timeout (int, optional): time of one session. Defaults to 10E25.
        """
        headers = {'Content-Type': 'application/json'}
        try:
            response = await session.post(url=self.url + endpoint,data=data, headers=headers,timeout=timeout)
        except:
            logger.error("Request Failed for this data :",json.dumps(data))

        filtered_response_json = None
        try:
            response_json = await response.json()
            filtered_response_json = self.filter_json_response(response_json)
        except json.decoder.JSONDecodeError as e:
            logger.exception("JSON Decoding Error")

        return filtered_response_json

    def filter_json_response(self,response_json: json):
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
        history_read_results = response_json.get('HistoryReadResults',[])
        response_json['HistoryReadResults'] = [self.filter_read_results(x) for x in history_read_results]
        return response_json

    def filter_read_results(self,x: Dict) -> Dict:
        """Function to filter HistoryReadResults' list of dictionaries

        Args:
            x (Dict): dict of json response 
        Returns:
            Dict: filtered dict
        """
        y = {}
        y['NodeId'] = x['NodeId']['Id']
        xvalues = x['DataValues']
        for v in xvalues:
            v['Value'] = v['Value']['Body']
        y['DataValues'] = xvalues
        return y

    def process_response_dataframe(self, df_result: pd.DataFrame):
        """This is a function to get historical aggregated data into require dataframe format

        Args:
            df_result (pd.DataFrame): api call's response for agg historical data request
        """
        df_result['Variable'] = df_result['NodeId'].str.split('.').str[-1]
        df_result1 = df_result.explode('DataValues').reset_index(drop=True)
        df_result2 = pd.json_normalize(df_result1.DataValues)
        df_merge = pd.concat([df_result1,df_result2], axis=1)
        df = df_merge.drop(columns=['DataValues']).set_axis(['Id','Variable', 'Value','Timestamp','Code','Quality'], axis=1)
        return df

    async def get_agg_hist_value_data(self,session: ClientSession, server_url: str, start_time: str, end_time: str, pro_interval: int, agg_name: str, obj_dataframe: pd.DataFrame, include_variables: List, chunk_size=100000,batch_size=1000,max_workers=50,timeout: int = 10E25):
        """Function to make aiohttp based multithreaded api requests to get aggregated historical data from opc ua api server and write the data in 'Data' folder in parquet files.

        Args:
            session (ClientSession): session of one request
            server_url (str): server connection url
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
        """
        node_ids = mdx.get_vars_node_ids(obj_dataframe)
        var_node_ids = [x for x in node_ids if (x.split(".")[-1]) in include_variables]
        read_value_ids = [self.create_readvalueids_dict(x,agg_name) for x in var_node_ids]
        # Lenght of time series
        n_datapoints = (pd.to_datetime(end_time) - pd.to_datetime(start_time)).total_seconds()*1000/pro_interval
        one_batch_datapoints = n_datapoints*batch_size
        # Number of required splits 
        n_time_splits = int(np.ceil(one_batch_datapoints/chunk_size))
        # Node ids chunks
        id_chunk_list = list(self.chunk_ids(read_value_ids, batch_size))
        # Get datetime chunks
        start_end_list = self.chunk_datetimes(start_time,end_time, n_time_splits)
        body_elements = list(itertools.product(start_end_list,id_chunk_list))
        # Create body chunks
        body_list = []
        for x in body_elements:
            start_time_new = x[0][0]
            end_time_new = x[0][1]
            ids = x[1]
            body = json.dumps({
                    "Connection": {
                        "Url": server_url,
                        "AuthenticationType": 1
                    },
                    "StartTime": start_time_new,
                    "EndTime": end_time_new,
                    "ProcessingInterval": pro_interval, 
                    "ReadValueIds": ids
                
                })
            body_list.append(body)
        endpoint = 'values/historicalaggregated'
        # Chunk body
        one_time_body_count = max_workers
        body_chunks_list = list(self.chunk_ids(body_list, one_time_body_count))
        # Folder to save the downloaded data
        Path("Data/").mkdir(exist_ok=True)
        logger.info("No. of Body Chunks : "+str(len(body_chunks_list)))
        for j,body_chunks in enumerate(body_chunks_list):
            logger.info("Requesting data for body chunk no. : "+str(j))
            # Request chunkwise data
            results = await asyncio.gather(*[self.http_get_with_aiohttp(session, endpoint,body,timeout) for body in body_chunks])
            # Create a dataframe and save as parquet
            results_list = []
            for res in results:
                for x in res['HistoryReadResults']:
                    results_list.append(x)
            df_results = pd.DataFrame(results_list)
            df = self.process_response_dataframe(df_results)
            df.to_parquet('Data/data_chunk_'+str(j)+'.parquet')
        logger.info(" Data donwload is complete ")
    