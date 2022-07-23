import requests
import json
import pandas as pd
from typing import List, Dict

class ModelIndex:
    """Data structure from the model index API server
    """
    def __init__(self, url: str):
        self.url = url
        self.object_types = self.get_object_types()

    def request(self, method: str, endpoint: str, data=None):       
        if method == 'GET':
            result = requests.get(self.url + endpoint)
        elif method == 'POST':
            result = requests.post(self.url + endpoint, data=data)
        else:
            raise Exception('Method not supported')
        if result.status_code == 200:
            return result.json()
        else:
            return None

    def get_namespace_array(self):
        return self.request('GET', 'query/namespace-array')

    def get_object_types(self):
        return self.request('GET', 'query/object-types')

    def get_object_typeid(self,type_name: str) -> str:
        """Function to get object type id from type name
        """
        try:
            obj_type =  next(item for item in self.object_types if item["BrowseName"] == type_name)
        except StopIteration:
            obj_type = {}
        object_type_id = obj_type.get("Id")
        return object_type_id

    def get_objects_of_type(self, type_name: str):
        object_type_id = self.get_object_typeid(type_name)
        body = json.dumps({"typeId": object_type_id})
        return self.request('POST', 'query/objects-of-type', body)

    def get_object_descendants(self, type_name: str, object_Ids: str, domain: str):
        """A function to get object descendants

        Args:
            type_name (str): type_name of a descendant
            object_Ids (List[str]): object id(s) of parent
            domain (str): PV_Assets or PV_Serves

        Returns:
            json data: descendats data of selected object id(s)
        """
        object_type_id = self.get_object_typeid(type_name)
        body = json.dumps({
            "typeId": object_type_id,
            "objectIds": object_Ids,
            "domain": domain
            })
        return self.request('POST', 'query/object-descendants', body)
 
    def get_object_ancestors(self, type_name: str, object_Ids: List[str], domain: str):
        """Function to get object ancestors

        Args:
            type_id (str): typeId of a parent type
            object_ids (List[str]): object id(s) of the descendants
            domain (str): Either PV_Assets or PV_Serves

        Returns:
            json data: ancestors data of selected object id(s)
        """
        object_type_id = self.get_object_typeid(type_name)
        body = json.dumps({
            "typeId": object_type_id,
            "objectIds": object_Ids,
            "domain": domain
            })
        return self.request('POST', 'query/object-ancestors', body)

    def get_vars_node_ids(self, objects_list: List[Dict]):
        """Variables node ids of the objects 

        Args:
            objects_list (List[Dict]): json object data from above functions calls  

        Returns:
            String: Node ids
        """        
        objects_vars = [x["Vars"] for x in objects_list]
        # Flatten the list
        vars_list = [x for xs in objects_vars for x in xs]
        vars_node_ids = [x["Id"] for x in vars_list]
        return vars_node_ids

    def expand_props_vars(self, json_data : List[Dict]) -> pd.DataFrame():
        """Convert json data into dataframe by expanding and merging back Vars and Props columns

        Args:
            json_data (List[Dict]): JSON response of API request 

        Returns:
            dataframe: Pandas dataframe 
        """
        # Convert json data to dataframe
        json_df = pd.DataFrame(json_data)
        # Make list of column names (in dataframe) except vars and props 
        non_vars_props_columns = [x for x in json_df.columns if x not in ['Vars','Props']]
        # New dataframe with exploded Props column and without Vars column
        json_df1 = json_df.drop(columns=['Vars']).explode('Props').reset_index(drop=True)
        # Make two new columns from expanded Props column
        json_df1[['Parameter','Value']] = json_df1['Props'].apply(pd.Series)
        json_df1 = json_df1.drop(columns=["Props"])
        # Create Pivot to convert parameter column into dataframe's columns
        json_df_props = json_df1.pivot(index=non_vars_props_columns,columns='Parameter',values='Value').reset_index()
        json_df_props.columns.name = None
        # Explode Vars and drop Props 
        json_df_vars = json_df.drop(columns=['Props']).explode('Vars').reset_index(drop=True)
        # Two new columns from expanded Vars column
        json_df_vars[['Variable','VariableId']] = json_df_vars['Vars'].apply(pd.Series)
        json_df_vars = json_df_vars.drop(columns=["Vars"])
        # Merge props and vars dataframes
        json_df_merged = json_df_vars.merge(json_df_props,on=non_vars_props_columns)
        return json_df_merged

