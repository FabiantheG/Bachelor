
from database.functions import *
from database.session import session
import pandas as pd
import numpy as np


def get_portfolio_data(assets, base_cur='CHF'):
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
            - df_fwd: DataFrame of forward FX hedge returns per currency (monthly frequency)
        :rtype: tuple[pd.DataFrame, list[str], pd.DataFrame, pd.DataFrame]
    """
    df_asset = pd.DataFrame()
    df_fwd = pd.DataFrame()
    df_spot = pd.DataFrame()
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
        fx[cur + 'forward'] = np.log(fx['forward'].shift(1)) - np.log(fx['spot'])  # Forward hedge return

        # Merge into spot and forward DataFrames
        df_spot = df_spot.join(fx[cur + 'spot'], how='outer').rename(columns={cur + 'spot': cur})
        df_fwd = df_fwd.join(fx[cur + 'forward'], how='outer').rename(columns={cur + 'forward': cur})


    return df_asset, cur_list, df_spot, df_fwd