from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd
import numpy as np


base = 'USD'
portfolio_name = 'portfolio' + base


data = get_portfolio_data(portfolio_name,base)


asset_logreturns = data[0]
currency_list = data[1]
fx_logreturns = data[2]
fwd_logreturns = data[3]
weights = data[4]
df_hedge = data[5]
factors = data[6]



ols = ols(currency_list,df_hedge,factors)

#print(ols)






#ols_simulation = xgboost(x[1],x[5], x[6],['dollar','carry','volatility'] )

#hedge_ratios = ols_simulation[0]

df = simulate_portfolio(asset_logreturns,currency_list,fx_logreturns,fwd_logreturns,weights,df_hedge,fx_portfolio = False)

plot_simulation(df)








#print(get_factor('dollarUSDCHF'))

