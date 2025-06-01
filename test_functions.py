#%% md
Unterstehend die erstellung von allen möglichen Optionen wie
#%%
from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from tqdm import tqdm
from simulation.functions import *
import pandas as pd
import numpy as np




base = 'USD'
portfolio_name = 'portfolio' + base




# Get Data
data = get_portfolio_data(portfolio_name,base)
asset_logreturns = data[0]
currency_list = data[1]
fx_logreturns = data[2]
fwd_logreturns = data[3]
weights = data[4]
df_hedge = data[5]
factors = data[6]

# Predict hedge Ratios
predict = ols(currency_list,
                    df_hedge,
                    factors,
                    duration =2)
hedge_ratios = predict[0]



df_fx = simulate_portfolio(asset_logreturns,
                           currency_list,
                           fx_logreturns,
                           fwd_logreturns,
                           weights,
                           hedge_ratios,
                           start='2005-10-31',
                           end='2024-12-31',
                           fx_portfolio=True)

df = df_fx
returns_model =   np.log(df['hedged_growth'] / df['hedged_growth'].shift(1))
returns_fully_hedged = np.log(df['fully_hedged_growth'] / df['fully_hedged_growth'].shift(1))
returns_unhedged = np.log(df['unhedged_growth'] / df['unhedged_growth'].shift(1))




excess_return = returns_model - np.minimum(returns_fully_hedged, returns_unhedged)
x = excess_return.cumsum()
x = pd.DataFrame(x)
x.columns = ['hallo']
plot_simulation(x)




3300-200 -100 -700



series = df['local_growth']