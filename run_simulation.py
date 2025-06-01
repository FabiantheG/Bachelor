from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd
import numpy as np

model = 'xgboost'
base = ''
portfolio_name = 'portfolio' + base
fx = True



title = model +' '+ base + ' FX' if fx else model +' '+ base
data = get_portfolio_data(portfolio_name,base)


asset_logreturns = data[0]
currency_list = data[1]
fx_logreturns = data[2]
fwd_logreturns = data[3]
weights = data[4]
df_hedge = data[5]
factors = data[6]

predict = xgboost(currency_list,
                  df_hedge,
                  factors,
                  duration =2)

hedge_ratios = predict[0]

df = simulate_portfolio(asset_logreturns,
                        currency_list,
                        fx_logreturns,
                        fwd_logreturns,
                        weights,
                        hedge_ratios,
                        start = '2005-10-31',
                        end = '2024-12-31',
                        fx_portfolio = fx)

plot_simulation(df,title,save = True)

print(predict)




