from ast import Str
import requests
import json
import pandas as pd
import numpy as np
from typing import List
import concurrent.futures
import itertools
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

    def get_live_values(self, s_url: str, node_ids: List[str]):
        """Request to get real time data values of the requested data for a site

        Args:
            s_url (str): server connection url
            node_ids (List[Dict]): node id(s) of the required data node(s)
        """
        node_ids_dicts = [self.split_node_id(x) for x in node_ids]
        body = json.dumps([
            {
                "Connection": {
                    "Url": s_url,
                    "AuthenticationType": 1
                },
                "NodeIds": node_ids_dicts
            }
        ])
        headers = {'Content-Type': 'application/json'}
        return self.request('POST', 'values/get', body,headers)


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

    def get_live_values_dataframe(self, server_url: str, data_frame : pd.DataFrame) -> pd.DataFrame:
        """Make a dataframe of the live values from the server 

        Args:
            server_url (str): server connection url
            data_frame (pd.DataFrame): Pandas data frame with required columns

        Returns:
            pd.DataFrame: Dataframe of the requested live values
        """
        # JSON normalization of the live values
        df = pd.json_normalize(self.get_live_values(server_url, data_frame['VariableId'].to_list())[0]['Values'])
        # Concating both the dataframes
        final_df = pd.concat([df,data_frame], axis=1)
        return final_df


    def get_agg_hist_values(self, server_url: str, start_time: str, end_time: str, pro_interval: int, agg_name: str, node_ids: List[str]):
        """Function to get historical aggregated time value json data for the selected site

        Args:
            server_url (str): server connection url
            start_time (str): start time of requested data
            end_time (str): end time of the requested data
            pro_interval (int): interval time of processing in milliseconds
            node_ids (List[Dict]): node id(s)
            agg_name (str): Name of aggregation
        """
        read_value_id_dict = [self.create_readvalueids_dict(x,agg_name) for x in node_ids]
        body = json.dumps({
                "Connection": {
                    "Url": server_url,
                    "AuthenticationType": 1
                },
                "StartTime": start_time,
                "EndTime": end_time,
                "ProcessingInterval": pro_interval, 
                "ReadValueIds": read_value_id_dict
            
            })
        headers = {'Content-Type': 'application/json'}
        return self.request('POST', 'values/historicalaggregated', body, headers)

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

    def request_historical_data(self,body):
        headers = {'Content-Type': 'application/json'}
        #try:
        resp = self.request('POST', 'values/historicalaggregated', body, headers)
        # except:
        #     resp = []
        #     print("Error while requesting historical aggregated data")
        return resp

    def get_agg_hist_value_chunks(self, server_url: str, start_time: str, end_time: str, pro_interval: int, agg_name: str, node_ids: List[str], chunk_size=100000,batch_size=1000, max_workers=2):
        """Function to get historical aggregated time value data for the selected site

        Args:
            server_url (str): server connection url
            start_time (str): start time of requested data
            end_time (str): end time of the requested data
            pro_interval (int): interval time of processing in milliseconds
            node_ids (List[Dict]): node id(s)
            agg_name (str): Name of aggregation
            chunk_size (int): size of each chunk, default : 100000
            batch_size (int): size of each Id chunck, default : 1000
            max_workers (int): maximum number of workers(CPU), default : 2
        """
        read_value_ids = [self.create_readvalueids_dict(x,agg_name) for x in node_ids]
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
        # Chunk body
        one_time_body_count = max_workers * 4
        body_chunks_list = list(self.chunk_ids(body_list, one_time_body_count))
        for j,body_chunks in enumerate(body_chunks_list):
        # Request chunkwise data
        #data_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_data_map = {executor.submit(self.request_historical_data, chunk):i for i,chunk in enumerate(body_chunks)}
                for i,future_data in enumerate(concurrent.futures.as_completed(future_data_map)):
                    print("Success: "+str(j)+"_"+str(i))
                    recieved_frame = future_data.result()
                    with open('data/data_chunk_'+str(j)+"_"+str(i)+'.json', 'w') as f:
                        json.dump(recieved_frame, f)
                    #data_list+=recieved_frame['HistoryReadResults']
            #return data_list


    def get_agg_hist_values_dataframe(self, server_url: str, start_time: str, end_time: str, pro_interval: int, agg_name: str, data_frame : pd.DataFrame) -> pd.DataFrame:
        """Make a dataframe of aggregated historical value data

        Args:
            server_url (str): server connection url
            start_time (str): start time of requested data
            end_time (str): end time of the requested data
            pro_interval (int): interval time of processing in milliseconds
            agg_name (str): Name of aggregation
            data_frame (pd.DataFrame): Pandas data frame with required columns such as node_ids

        Returns:
            pd.DataFrame: Dataframe of the requested historical values
        """
        # JSON normalization of aggregate historical values
        df = pd.json_normalize(self.get_agg_hist_value_chunks(server_url, start_time, end_time, pro_interval, agg_name, data_frame['VariableId']))
        # Concating aggregated historical values to dataframe
        data_frame1 = pd.concat([data_frame,df], axis=1).drop(columns=['NodeId.IdType', 'NodeId.Id', 'NodeId.Namespace', 'StatusCode.Code', 'StatusCode.Symbol']) 
        # Exploding DataValues column
        df1 = data_frame1.explode('DataValues').reset_index(drop=True)
        # JSON normalization of DataValues
        df2 = pd.json_normalize(df1['DataValues'])
        # Concatenating dataframes
        data_frame2 = pd.concat([df1,df2], axis=1).drop(columns=["DataValues"])
        return data_frame2
    