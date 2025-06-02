from database.functions import *
import pandas as pd
import numpy as np
from simulation.functions import *


def hyper_analysis(model,list_with_versions):

    new_df = pd.DataFrame(index = list_with_versions)

    for x in list_with_versions:
        df = get_mean_df(model,x)
        last_entry = df['hedged_growth'].iloc[-1]
        new_df.loc[x,'last_entry'] = last_entry


    return new_df






