
from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_cpi(currency):
    series_nr = session.query(CPI_REF.series_id).filter_by(currency=currency).first()

    if not series_nr:
        print("Series not found")
        return None

    else:
        new = session.query(CPI_TS).filter_by(series_id=series_nr[0]).all()
        df = orm_list_to_df(new)
        df = df.drop(columns=["series_id"])



    return df