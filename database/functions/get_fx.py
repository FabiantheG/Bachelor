from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *


def get_fx_usd(base,quote,duration):
    series_nr = session.query(FX_REF.series_id).filter_by(base_cur=base,quote_cur = quote,duration = duration).first()

    if not series_nr:
        series_nr2 = session.query(FX_REF.series_id).filter_by(base_cur=quote, quote_cur=base, duration=duration).first()
        if not series_nr2:
            print("Series not found")
            return None

        else:
            new = session.query(FX_TS).filter_by(series_id=series_nr2[0]).all()
            df = orm_list_to_df(new)
            df = df.drop(columns=["series_id"])
            df['rate'] = 1/ df['rate']

    else: # if there is the right quotation at the beginning
        new = session.query(FX_TS).filter_by(series_id=series_nr[0]).all()
        df = orm_list_to_df(new)
        df = df.drop(columns=["series_id"])

    return df #return the dataframe


def get_fx(base,quote,duration):
    if base == 'USD' or quote == 'USD':
        df = get_fx_usd(base,quote,duration)

    else:
        df1 = get_fx_usd(base,'USD',duration)
        df2 = get_fx_usd('USD',quote,duration)
        df1['rate'] = df1['rate'] * df2['rate']
        df = df1

    return  df






#print(get_fx_usd('CHF','USD','Spot'))
#print(get_fx('JPY','USD','Spot'))



