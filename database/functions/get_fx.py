from database.models import *
from database.session import session
from database.functions import *
from database.functions.orm_list_to_df import orm_list_to_df

def get_fx_usd(base, quote, duration):
    """
    Load USD-based FX rates as a pandas DataFrame.

    Queries the database for the FX series matching the given base and quote currencies
    and duration. If the series is quoted as quote/base, it inverts the rate.

    :param base: Base currency code.
    :type base: str
    :param quote: Quote currency code.
    :type quote: str
    :param duration: Duration identifier (e.g., 'Spot', '1M').
    :type duration: str
    :return: pandas DataFrame with columns ['date', 'rate'], or None if no series is found.
    :rtype: pandas.DataFrame or None
    """
    series_nr = session.query(FX_REF.series_id) \
                       .filter_by(base_cur=base, quote_cur=quote, duration=duration) \
                       .first()

    if not series_nr:
        # Try inverted quotation
        series_nr2 = session.query(FX_REF.series_id) \
                            .filter_by(base_cur=quote, quote_cur=base, duration=duration) \
                            .first()
        if not series_nr2:
            print("Series not found")
            return None
        # Load and invert rates
        new = session.query(FX_TS).filter_by(series_id=series_nr2[0]).all()
        df = orm_list_to_df(new)
        df = df.drop(columns=["series_id"])
        df['rate'] = 1 / df['rate']
    else:
        # Load direct rates
        new = session.query(FX_TS).filter_by(series_id=series_nr[0]).all()
        df = orm_list_to_df(new)
        df = df.drop(columns=["series_id"])

    return df


def get_fx(base, quote, duration):
    """
    Retrieve FX rates for any currency pair via USD.

    If either base or quote is USD, it calls `get_fx_usd` directly.
    Otherwise, it fetches base/USD and USD/quote series, multiplies the rates,
    and returns the combined series.

    :param base: Base currency code.
    :type base: str
    :param quote: Quote currency code.
    :type quote: str
    :param duration: Duration identifier (e.g., 'Spot', '1M').
    :type duration: str
    :return: pandas DataFrame with columns ['date', 'rate'], or None if no series is found.
    :rtype: pandas.DataFrame or None
    """
    if base == 'USD' or quote == 'USD':
        df = get_fx_usd(base, quote, duration)
    else:
        df1 = get_fx_usd(base, 'USD', duration)
        df2 = get_fx_usd('USD', quote, duration)
        if df1 is None or df2 is None:
            return None
        df1['rate'] = df1['rate'] * df2['rate']
        df = df1

    return df
