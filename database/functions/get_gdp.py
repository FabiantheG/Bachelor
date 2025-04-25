from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_gdp(country):
    """
    Load GDP time series as a pandas DataFrame.

    Queries the database for the GDP series corresponding to the given country code,
    converts the ORM result into a pandas DataFrame, and removes the 'series_id' column.

    :param country: ISO country code for which to retrieve the GDP series.
    :type country: str
    :return: pandas DataFrame containing the GDP time series data, or None if no series is found.
    :rtype: pandas.DataFrame or None
    """
    series_nr = session.query(GDP_REF.series_id) \
                       .filter_by(country=country) \
                       .first()

    if not series_nr:
        print("Series not found")
        return None

    new = session.query(GDP_TS) \
                 .filter_by(series_id=series_nr[0]) \
                 .all()
    df = orm_list_to_df(new)
    df = df.drop(columns=["series_id"])

    return df
