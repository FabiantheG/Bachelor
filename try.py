from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd
import numpy as np
from tqdm import tqdm




liste = [
['ted', 'volatility', 'afd', 'commodity'],
['ted', 'volatility', 'afd'],
['ted', 'volatility',  'commodity'],
['ted', 'afd', 'commodity'],
[ 'volatility', 'afd', 'commodity'],
[ 'afd', 'commodity'],
['ted', 'volatility'],
[ 'volatility', 'afd'],
['ted', 'commodity']
]
version = 10
for l in tqdm(liste):
    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK', 'USD']

    model = 'XGBOOST'


    for cur in tqdm(g10):
        base = cur
        portfolio_name = 'portfolio' + base
        portfolio_name_fx = 'portfolioFX' + base

        # Get Data
        data = get_portfolio_data(portfolio_name, base)
        asset_logreturns = data[0]
        currency_list = data[1]
        fx_logreturns = data[2]
        fwd_logreturns = data[3]
        weights = data[4]
        df_hedge = data[5]
        factors = data[6]

        # Predict hedge Ratios
        predict = xgboost(currency_list,
                          df_hedge,
                          factors,
                          duration=2,
                          pred = l)
        hedge_ratios = predict[0]

        # Simulate Portfolio
        df_fx = simulate_portfolio(asset_logreturns,
                                   currency_list,
                                   fx_logreturns,
                                   fwd_logreturns,
                                   weights,
                                   hedge_ratios,
                                   start='2005-10-31',
                                   end='2024-12-31',
                                   fx_portfolio=True)

        df_asset = simulate_portfolio(asset_logreturns,
                                      currency_list,
                                      fx_logreturns,
                                      fwd_logreturns,
                                      weights,
                                      hedge_ratios,
                                      start='2010-07-31',
                                      end='2024-12-31',
                                      fx_portfolio=False)

        # save into DB
        insert_simulation(portfolio_name, model, version=version, df=df_asset)
        insert_simulation(portfolio_name_fx, model, version=version, df=df_fx)


    print(l,version)
    version = version + 1