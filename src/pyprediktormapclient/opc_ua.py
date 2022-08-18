from ast import Str
import requests
import json
import pandas as pd
import numpy as np
from typing import List
import concurrent.futures
import itertools
from pathlib import Path

import asyncio
import time
from typing import Dict, Any, List, Tuple
from itertools import repeat
from aiohttp import ClientSession
import calendar
  

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
        """Functions to get node id with namespace index and data format of the node  

        Args:
            id_string (str): node id of a node

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

    def get_live_values(self, server_url: str, include_variables: List, node_ids: List[str]):
        """Request to get real time data values of the requested data for a site

        Args:
            server_url (str): server connection url
            node_ids (List[Dict]): node id(s) of the required data node(s)
        """
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
        return self.request('POST', 'values/get', body,headers)

    def get_live_values_dataframe(self, server_url: str, include_variables: List, data_frame : pd.DataFrame) -> pd.DataFrame:
        """Make a dataframe of the live values from the server 

        Args:
            server_url (str): server connection url
            data_frame (pd.DataFrame): Pandas data frame with required columns

        Returns:
            pd.DataFrame: Dataframe of the requested live values
        """
        # JSON normalization of the live values
        df = pd.json_normalize(self.get_live_values(server_url, include_variables, data_frame['VariableId'].to_list())[0]['Values'])
        # Filtering dataframe for the variables
        data_frame1 = data_frame[data_frame['Variable'].isin(include_variables)].reset_index(drop=True)
        # Concating both the dataframes
        final_df = pd.concat([data_frame1,df], axis=1)
        return final_df

    def create_readvalueids_dict(self, node_id: str, agg_name: str):
        """A function to get ReadValueIds

        Args:
            id_string (str): node id of a node 
            agg_name (str): Name of aggregation

        Returns:
            Dict: node id(s) with aggregation type name
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
            start_time (_type_): Time from (start date)
            end_time (_type_): Time to (end date)
            n_time_splits (_type_): Number of splits

        Returns:
            List: List of datetimes tuples
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

    ###############New Functions
    async def http_get_with_aiohttp(self,session: ClientSession, endpoint: str,data, timeout: int = 10):
        """Request function for aiohttp based API request

        Args:
            session (ClientSession): ClientSession of a period of time for the reqested data
            endpoint (str): Endpoint of requested API reqest
            data (_type_): Body data
            timeout (int, optional): Time of one session. Defaults to 10.

        Returns:
            _type_: _description_
        """
        headers = {'Content-Type': 'application/json'}
        response = await session.post(url=self.url + endpoint,data=data, headers=headers,timeout=timeout)

        response_json = None
        try:
            response_json = await response.json()
        except json.decoder.JSONDecodeError as e:
            pass

        # # ts stores timestamp
        # ts = calendar.timegm(time.gmtime())
        # with open('data/data_chunk_'+str(ts)+'.json', 'w') as f:
        #     json.dump(response_json, f)
        filtered_response_json = self.filter_json_response(response_json)
        return filtered_response_json

    def filter_json_response(self,response_json):
        response_json.pop("ServerNamespaces", None)
        response_json.pop("Success", None)
        response_json['HistoryReadResults'] = [self.filter_read_results(x) for x in response_json['HistoryReadResults']]
        return response_json

    def filter_read_results(self,x):
        y = {}
        y['NodeId'] = x['NodeId']['Id']
        xvalues = x['DataValues']
        for v in xvalues:
            v['Value'] = v['Value']['Body']
        y['DataValues'] = xvalues
        return y

    def json_data_into_dataframe(self, df_result: pd.DataFrame):
        df_result['Variable'] = df_result['NodeId'].str.split('.').str[-1]
        df_result1 = df_result.explode('DataValues').reset_index(drop=True)
        df_result1[['Value', 'StatusCode', 'SourceTimestamp']] = df_result1['DataValues'].apply(pd.Series)
        df_result1[['Code', 'Status']] = df_result1['StatusCode'].apply(pd.Series)
        df = df_result1.drop(columns=['DataValues','StatusCode'])
        return df

    async def get_agg_hist_value_chunks_parallel(self,session: ClientSession, server_url: str, start_time: str, end_time: str, pro_interval: int, agg_name: str, node_ids: List[str], include_variables: List, chunk_size=100000,batch_size=1000,max_workers=4,timeout: int = 10E15):
        """Function to make aiohttp based multithreaded API request to get aggregated historical data from OPC UA API

        Args:
            session (ClientSession): Session of one request
            server_url (str): server connection url
            start_time (str): start time of requested data
            end_time (str): end time of requested data
            pro_interval (int): interval time of processing in milliseconds
            agg_name (str): Name of aggregation
            node_ids (List[str]): node id(s)
            chunk_size (int, optional): Time chunk size. Defaults to 100000.
            batch_size (int, optional): size of each Id chunck. Defaults to 1000.
            max_workers (int, optional): maximum number of workers(CPU). Defaults to 10.
            timeout (int, optional): Timeout time of one session. Defaults to 10.
        """
        var_node_ids = [x for x in node_ids if (x.split(".")[-1]) in include_variables]
        read_value_ids = [self.create_readvalueids_dict(x,agg_name) for x in var_node_ids]
        # Lenght of time series
        n_datapoints = (pd.to_datetime(end_time) - pd.to_datetime(start_time)).total_seconds()*1000/pro_interval
        # Number of required splits 
        n_time_splits = int(np.ceil(n_datapoints/chunk_size))
        # Ids chunks
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
        #
        endpoint = 'values/historicalaggregated'
        # Chunk body
        one_time_body_count = max_workers
        body_chunks_list = list(self.chunk_ids(body_list, one_time_body_count))
        # Folder to save the data
        Path("data/").mkdir(exist_ok=True)
        for j,body_chunks in enumerate(body_chunks_list):
        # Request chunkwise data
            results = await asyncio.gather(*[self.http_get_with_aiohttp(session, endpoint,body,timeout) for body in body_chunks])
            print("Success: "+str(j))
            #with open('data/data_chunk_'+str(j)+'.json', 'w') as f:
            #    json.dump(results, f)
            # Create a dataframe and save as parquet
            results_list = []
            for res in results:
                for x in res['HistoryReadResults']:
                    results_list.append(x)
        # return pd.json_normalize(results_list)
            df_results = pd.DataFrame(results_list)
            # df = self.json_data_into_dataframe(df_results)
            df_results.to_parquet('data/data_chunk_'+str(j)+'.parquet')
    