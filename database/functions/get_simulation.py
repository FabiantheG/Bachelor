from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *


def get_simulation(name, version):
    sim_name_version = session.query(HEDGING_STRATEGY).filter_by(name=name, version = version).first()
    if not sim_name_version:
        print(f"Hedging strategy '{name}' or '{version}' not found.")
        return

    else:
        hedge_id = session.query(HEDGING_STRATEGY.hedge_id).filter_by(name=name, version=version).first()
        sim_id = session.query(SIMULATION_REF.simulation_id).filter_by(hedge_id= hedge_id).first()
        new = session.query(SIMULATION_TS).filter_by(simulation_id = sim_id).all()
        df = orm_list_to_df(new, date_index = False)
        df = df.drop(columns=["unhedged_growth", "hedged_growth"])
    return df



#get_simulation('DCF_Hedge',1.2)


