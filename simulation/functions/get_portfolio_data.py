
from database.functions import *
from database.session import session
import pandas as pd
import numpy as np
from simulation.functions.get_factors_simulation import  get_factors_simulation

def get_portfolio_data(portfolio_name, base_cur='CHF',factor_list= ['dollar','carry']):

    """
        Load asset and FX time series for a given list of assets.

        For each asset, retrieves its price data and associated currency from the database,
        computes log returns of asset prices, and fetches corresponding spot and 1-month forward FX rates
        relative to a base currency. Returns log returns of assets, their currencies, and FX log returns.
        (spot and forward FX  logreturns)

        :param assets: List of asset tickers to include in the portfolio.
        :type assets: list[str]
        :param base_cur: Base currency (e.g., 'CHF') against which FX rates are expressed.
        :type base_cur: str
        :return: Tuple containing:
            - df_asset: DataFrame of log returns of asset prices (monthly frequency)
            - cur_list: List of currencies corresponding to each asset (in order)
            - df_spot: DataFrame of spot FX log returns per currency (monthly frequency)
            - df_fwd: DataFrame of forward FX returns per currency (monthly frequency)
        :rtype: tuple[pd.DataFrame, list[str], pd.DataFrame, pd.DataFrame]
    """
    portfolio = session.query(PORTFOLIO).filter_by(portfolio_name=portfolio_name).first()
    id = portfolio.portfolio_id
    asset_connections = session.query(PORTFOLIO_ASSET_CONNECTION).filter_by(portfolio_id=id).all()
    assets = [row.asset_ticker for row in asset_connections]
    weights = [row.weight for row in asset_connections]


    df_asset = pd.DataFrame()
    df_fwd = pd.DataFrame()
    df_spot = pd.DataFrame()
    df_hedge = pd.DataFrame()

    cur_list = []

    # Loop over all assets
    for asset in assets:
        # Get asset currency
        n = session.query(ASSET).filter_by(asset_ticker=asset).first()
        currency = n.currency
        cur_list.append(currency)

        # Get asset price data
        df_asset_fun = get_asset(asset)  # DataFrame with 'Date' index and 'close' column
        df_asset_fun = df_asset_fun.resample('M').last()  # Use monthly data (last observation)
        df_asset_fun = df_asset_fun.rename(columns={'close': asset})  # Rename 'close' to asset ticker

        # Merge into main asset DataFrame
        df_asset = df_asset.join(df_asset_fun, how='outer')

    # Calculate log returns for assets
    df_asset = np.log(df_asset / df_asset.shift(1))


    cur_list_no_duplicates = list(set(cur_list))
    # Loop over all currencies
    for cur in cur_list_no_duplicates:
        # Fetch spot and forward FX rates
        spot = get_fx(base=cur, quote=base_cur, duration='Spot').rename(columns={'rate': 'spot'})
        forward = get_fx(base=cur, quote=base_cur, duration='1M').rename(columns={'rate': 'forward'})

        # Merge spot and forward data
        fx = pd.merge(spot, forward, left_index=True, right_index=True)
        fx = fx.resample('M').last()  # Monthly data

        # Calculate log returns
        fx[cur + 'spot'] = np.log(fx['spot'] / fx['spot'].shift(1))  # Spot return
        fx[cur + 'forward'] = np.log(fx['forward'].shift(1)) - np.log(fx['spot'].shift(1))  # Forward hedge return
        fx[cur + 'hedge'] = fx[cur + 'forward'] - fx[cur + 'spot']



        # Merge into spot and forward DataFrames
        df_spot = df_spot.join(fx[cur + 'spot'], how='outer').rename(columns={cur + 'spot': cur})
        df_fwd = df_fwd.join(fx[cur + 'forward'], how='outer').rename(columns={cur + 'forward': cur})
        df_hedge = df_hedge.join(fx[cur + 'hedge'], how='outer').rename(columns={cur + 'hedge': cur})



    factors = get_factors_simulation(base = base_cur,cur_list= cur_list,list_factors=factor_list)

    return df_asset, cur_list, df_spot, df_fwd, weights, df_hedge, factors