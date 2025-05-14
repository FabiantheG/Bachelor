from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd
import numpy as np


base = 'CHF'


portfolio_name = 'portfolio' + base
x = get_portfolio_data(portfolio_name,base)

dollar  = create_dollar_factor(base)
carry = create_carry_factor(base)




ratio_df = ols(x[1],x[3],carry,dollar)
#ratio_df = pd.DataFrame(1, index=x[0].index, columns=x[1])







df = simulate_portfolio(x[0],x[1],x[2],x[3],x[4],hedge_ratio = ratio_df,fx_portfolio = True)

#plot_simulation(df)

g10 = ['EUR', 'JPY',  'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK','USD']

df = pd.DataFrame()
for cur in g10:
    spotrate = get_fx(cur,'CHF','Spot')
    fwdrate = get_fx(cur,'CHF','1M')


    df[cur+'CHFspot'] = spotrate
    df[cur+'CHFforward'] = fwdrate

df  = df.resample('M').last()
df.to_excel('rates.xlsx')



