
from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_asset(asset_ticker):
    series_nr = session.query(ASSET_REF.series_id).filter_by(asset_ticker=asset_ticker).first()

    if not series_nr:
        print("Series not found")
        return None

    else:
        new = session.query(ASSET_TS).filter_by(series_id=series_nr[0]).all()
        df = orm_list_to_df(new)
        df = df.drop(columns=["series_id"])



    return df


