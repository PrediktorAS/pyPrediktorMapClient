from ast import Str
import requests
import json
import pandas as pd
from typing import List

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

    def get_values_dataframe(self, server_url: str, data_frame : pd.DataFrame) -> pd.DataFrame:
        """Make a dataframe of the live values from the server 

        Args:
            server_url (str): server connection url
            data_frame (pd.DataFrame): Pandas data frame with required columns

        Returns:
            pd.DataFrame: Dataframe of the requested live values
        """
        # Make a new column of live values 
        data_frame['LiveValue'] = self.get_live_values(server_url, data_frame['VariableId'].to_list())[0]['Values']
        # Expand live values and make three columns
        data_frame[['Value','SourceTimestamp','ServerTimestamp']] = data_frame['LiveValue'].apply(pd.Series)
        # Further expand Value column
        data_frame[['Type','Body']] = data_frame['Value'].apply(pd.Series)
        data_frame = data_frame.drop(columns=['LiveValue','Value'])
        return data_frame


    def get_agg_hist_values(self, s_url: str, start_time: str, end_time: str, pro_interval: int, agg_name: str, node_ids: List[str]):
        """Function to get historical aggregated time value data for the selected site

        Args:
            s_url (str): server connection url
            start_time (str): start time of requested data
            end_time (str): end time of the requested data
            pro_interval (int): interval time of processing in milliseconds
            node_ids (List[Dict]): node id(s)
            agg_name (str): Name of aggregation
        """
        read_value_id_dict = [self.create_readvalueids_dict(x,agg_name) for x in node_ids]
        body = json.dumps({
                "Connection": {
                    "Url": s_url,
                    "AuthenticationType": 1
                },
                "StartTime": start_time,
                "EndTime": end_time,
                "ProcessingInterval": pro_interval, 
                "ReadValueIds": read_value_id_dict
            
            })
        headers = {'Content-Type': 'application/json'}
        return self.request('POST', 'values/historicalaggregated', body, headers)

    def get_aggHist_values_dataframe(self, server_url: str, start_time: str, end_time: str, p_interval: int, agg_name: str, data_frame : pd.DataFrame) -> pd.DataFrame:
        """Make a dataframe of aggregated historical value data

        Args:
            server_url (str): server connection url
            start_time (str): start time of requested data
            end_time (str): end time of the requested data
            p_interval (int): interval time of processing in milliseconds
            agg_name (str): Name of aggregation
            data_frame (pd.DataFrame): Pandas data frame with required columns

        Returns:
            pd.DataFrame: Dataframe of the requested historical values
        """
        # Make a new column of aggregate historical values
        data_frame['AggHistValue'] = self.get_agg_hist_values(server_url, start_time, end_time, p_interval, agg_name, data_frame['VariableId'].to_list())['HistoryReadResults']
        # Expand values and make three columns
        data_frame[['NodeId', 'StatusCode', 'DataValues']] = data_frame['AggHistValue'].apply(pd.Series)
        # Explode DataValues
        data_frame = data_frame.drop(columns=["AggHistValue", 'NodeId', 'StatusCode']).explode('DataValues').reset_index(drop=True)
        data_frame[['Value', 'StatusCode', 'SourceTimestamp']] = data_frame['DataValues'].apply(pd.Series)
        # Further expand Value and StatusCode columns
        data_frame[['Type','Body']] = data_frame['Value'].apply(pd.Series)
        data_frame[['Code', 'Quality']] = data_frame['StatusCode'].apply(pd.Series)
        data_frame = data_frame.drop(columns=['DataValues','Value', 'StatusCode'])
        return data_frame

    