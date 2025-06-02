from database.functions import *
import pandas as pd
import numpy as np


def get_mean_df(model,version = 1):
    df = get_simulation(portfolio_name='portfolioFXUSD', hedge_name=model, version=version)

    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK']

    for x in g10:
        current = get_simulation(portfolio_name='portfolioFX' + x, hedge_name=model, version=version)
        df = df + current

    df = df.dropna()
    df = df / df.iloc[0]
    return df