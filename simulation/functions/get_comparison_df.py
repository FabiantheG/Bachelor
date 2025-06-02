from database.functions import *
import pandas as pd
import numpy as np
from simulation.functions import *





def get_comparison_df(liste):
    '''
    liste = [['XGBOOST',10,'name1'],['XGBOOST',11,'name2']]

    namen = ['name1', 'name2']
    versionen = [1,4,54,435,34,43,2]

    liste = [['XGBOOST',version,name] for version, name in versionen, namen]
    :param liste:
    :return:
    '''
    comparison_list = []
    for l in liste:
        model = l[0]
        version = l[1]
        name = l[2]
        current_df = get_mean_df(model = model,version = version)
        current_df = current_df[['hedged_growth']]

        current_df.columns = [name]
        comparison_list.append(current_df)

    comparison_df = pd.concat(comparison_list,axis = 1, join = 'inner')

    return comparison_df