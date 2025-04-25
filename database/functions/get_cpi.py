from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_cpi(currency):
    """
    Load CPI time series as a pandas DataFrame.

    Queries the database for the series corresponding to the given currency code,
    converts the ORM result into a pandas DataFrame, and removes the 'series_id' column.

    :param currency: ISO currency code for which to retrieve the CPI series.
    :type currency: str
    :return: pandas DataFrame containing the CPI time series data, or None if no series is found.
    :rtype: pandas.DataFrame or None
    """
    series_nr = session.query(CPI_REF.series_id)\
                       .filter_by(currency=currency)\
                       .first()

    if not series_nr:
        print("Series not found")
        return None

    new = session.query(CPI_TS)\
                 .filter_by(series_id=series_nr[0])\
                 .all()
    df = orm_list_to_df(new)
    df = df.drop(columns=["series_id"])

    return df
