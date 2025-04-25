from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_asset(asset_ticker):
    """
    Load asset time series as a pandas DataFrame.

    Queries the database for the series corresponding to the given asset ticker,
    converts the ORM result into a pandas DataFrame, and removes the 'series_id' column.

    :param asset_ticker: Unique ticker symbol of the asset to retrieve.
    :type asset_ticker: str
    :return: pandas DataFrame containing the asset’s time series data, or None if no series is found.
    :rtype: pandas.DataFrame or None
    """
    series_nr = session.query(ASSET_REF.series_id)\
                       .filter_by(asset_ticker=asset_ticker)\
                       .first()

    if not series_nr:
        print("Series not found")
        return None

    new = session.query(ASSET_TS)\
                 .filter_by(series_id=series_nr[0])\
                 .all()
    df = orm_list_to_df(new)
    df = df.drop(columns=["series_id"])

    return df
