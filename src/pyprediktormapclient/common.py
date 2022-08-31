import pandas as pd
from typing import List

def get_vars_node_ids(obj_dataframe: pd.DataFrame) -> List:
    """Function to get variables node ids of the objects

    Args:
        obj_dataframe (pd.DataFrame): object dataframe
    Returns:
        List: list of variables' node ids
    """                
    objects_vars = obj_dataframe["Vars"]
    # Flatten the list
    vars_list = [x for xs in objects_vars for x in xs]
    vars_node_ids = [x["Id"] for x in vars_list]
    return vars_node_ids

def expand_props_vars(json_df: pd.DataFrame):
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