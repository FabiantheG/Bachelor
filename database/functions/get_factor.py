from statsmodels.multivariate.factor import Factor

from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *


def get_factor(name,version = 1):


    factor = session.query(FACTOR).filter_by(name_factor= name , version = version).first()
    if factor is None:
        print("Factor not found")
        return None
    else:
        fac_ref = session.query(FACTOR_REF).filter_by(factor_id = factor.factor_id).first()
        if fac_ref is None:
            print('no ref found')
            return None
        else:

            new = session.query(FACTOR_TS).filter_by(series_id=fac_ref.series_id).all()

            df = orm_list_to_df(new)
            df = df.drop(columns=["series_id"])

    return df

