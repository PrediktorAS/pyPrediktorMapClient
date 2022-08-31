import pandas as pd
from typing import List

def get_vars_node_ids(self, obj_dataframe: pd.DataFrame) -> List:
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
