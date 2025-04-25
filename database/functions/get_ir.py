from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_ir(currency):
    """
    Load interest rate time series as a pandas DataFrame.

    Queries the database for the interest rate ID corresponding to the given currency,
    then retrieves the associated series, converts it into a pandas DataFrame,
    and removes the 'series_id' column.

    :param currency: ISO currency code for which to retrieve the interest rate series.
    :type currency: str
    :return: pandas DataFrame containing the interest rate time series data, or None if no series is found.
    :rtype: pandas.DataFrame or None
    """
    ir_id = session.query(INTEREST_RATE.ir_id).filter_by(currency=currency).first()
    series_nr = session.query(IR_REF.series_id).filter_by(ir_id=ir_id[0]).first()

    if not series_nr:
        print("Series not found")
        return None

    new = session.query(IR_TS).filter_by(series_id=series_nr[0]).all()
    df = orm_list_to_df(new)
    df = df.drop(columns=["series_id"])

    return df
