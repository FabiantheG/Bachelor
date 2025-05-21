from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd
import numpy as np


base = 'CHF'
portfolio_name = 'portfolio' + base


data = get_portfolio_data(portfolio_name,base,['dollar','carry','volatility'])


asset_logreturns = data[0]
currency_list = data[1]
fx_logreturns = data[2]
fwd_logreturns = data[3]
weights = data[4]
df_hedge = data[5]
factors = data[6]



#dollar  = create_dollar_factor(base)
#carry = create_carry_factor(base)

#factors = pd.concat([dollar,carry], axis=1, join='inner')


ols_simulation = xgboost(x[1],x[5], x[6],['dollar','carry','volatility'] )

hedge_ratios = ols_simulation[0]

df = simulate_portfolio(x[0],x[1],x[2],x[3],x[4],hedge_ratio = hedge_ratios,fx_portfolio = True)

plot_simulation(df)


print(x[6])





#print(get_factor('dollarUSDCHF'))

