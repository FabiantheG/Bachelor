

from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd
import numpy as np
from tqdm import tqdm


model = 'XGBOOST'

list_with_versions = [x for x in range(10,19)]  #  versionen 10 bis 19 (immer minus 1)

df = hyper_analysis(model, list_with_versions)

# jetzt siehst du die letzten Werte also die besten versionen
print(df)


liste = [['XGBOOST',10,'name1'],['XGBOOST',11,'name2']]

new_df = get_comparison_df(liste)

plot_simulation(new_df)

