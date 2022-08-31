import requests
import json
import pandas as pd

import logging

logger = logging.getLogger()


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
        return pd.DataFrame(self.request('GET', 'query/namespace-array'))

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
        """Function to get all the types of an object

        Args:
            type_name (str): type name 
        """
        object_type_id = self.get_object_typeid(type_name)
        body = json.dumps({"typeId": object_type_id})
        return pd.DataFrame(self.request('POST', 'query/objects-of-type', body))

    def get_object_descendants(self, type_name: str, obj_dataframe: pd.DataFrame, domain: str) -> pd.DataFrame:
        """A function to get object descendants

        Args:
            type_name (str): type_name of a descendant
            obj_dataframe (pd.DataFrame): dataframe of object ids
            domain (str): PV_Assets or PV_Serves

        Returns:
            pd.DataFrame: descendats data of selected object
        """
        object_type_id = self.get_object_typeid(type_name)
        id_column = [x for x in obj_dataframe if x in ['Id','DescendantId', 'AncestorId']][0]
        object_Ids = obj_dataframe[id_column].to_list()
        body = json.dumps({
            "typeId": object_type_id,
            "objectIds": object_Ids,
            "domain": domain
            })
        return pd.DataFrame(self.request('POST', 'query/object-descendants', body))

    def get_object_ancestors(self, type_name: str, obj_dataframe: pd.DataFrame, domain: str) -> pd.DataFrame:
        """Function to get object ancestors

        Args:
            type_name (str): type_name of a parent type
            obj_dataframe (pd.DataFrame): dataframe of object ids
            domain (str): Either PV_Assets or PV_Serves

        Returns:
            pd.DataFrame: ancestors data of selected object
        """
        object_type_id = self.get_object_typeid(type_name)
        id_column = [x for x in obj_dataframe if x in ['Id','AncestorId', 'DescendantId']][0]
        object_Ids = obj_dataframe[id_column].to_list()
        body = json.dumps({
            "typeId": object_type_id,
            "objectIds": object_Ids,
            "domain": domain
            })
        return pd.DataFrame(self.request('POST', 'query/object-ancestors', body))

    def expand_props_vars(self, json_df: pd.DataFrame):
        """Get a dataframe with required columns by expanding and merging back Vars and Props columns

        Args:
            json_df (pd.DataFrame): json dataframe of API request
        Returns:
            dataframe: Pandas dataframe
        """
        # Make list of column names except vars and props 
        non_vars_props_columns = [x for x in json_df.columns if x not in ['Vars','Props']]
        json_df1 = json_df.explode('Props').reset_index(drop=True)
        json_df1[['Parameter','Value']] = json_df1['Props'].apply(pd.Series)
        json_df1 = json_df1.drop(columns=['Props','Vars'])
        # Create Pivot to convert parameter column into dataframe's columns
        json_df_props = json_df1.pivot(index=non_vars_props_columns,columns='Parameter',values='Value').reset_index()
        json_df_props.columns.name = None
        json_df_vars = json_df.explode('Vars').reset_index(drop=True)
        json_df_vars[['Variable','VariableId']] = json_df_vars['Vars'].apply(pd.Series)
        json_df_vars = json_df_vars.drop(columns=['Vars', 'Props'])
        # Merge props and vars dataframes
        json_df_merged = json_df_vars.merge(json_df_props,on=non_vars_props_columns)
        return json_df_merged