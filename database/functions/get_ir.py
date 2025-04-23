from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_ir(currency):
    ir_id = session.query(INTEREST_RATE.ir_id).filter_by(currency=currency).first()
    series_nr = session.query(IR_REF.series_id).filter_by(ir_id=ir_id[0]).first()

    if not series_nr:
        print("Series not found")
        return None

    else:
        new = session.query(IR_TS).filter_by(series_id=series_nr[0]).all()
        df = orm_list_to_df(new)
        df = df.drop(columns=["series_id"])

    return df