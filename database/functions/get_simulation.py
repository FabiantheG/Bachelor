from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_simulation(portfolio_name, hedge_name, version):
    """
    Load simulation time series for a given portfolio and hedging strategy.

    This function retrieves the portfolio and hedging strategy by name and version,
    finds the corresponding simulation reference, and returns the hedged and
    unhedged growth series as a pandas DataFrame indexed by date.

    :param portfolio_name: Name of the portfolio to simulate.
    :type portfolio_name: str
    :param hedge_name: Name of the hedging strategy to apply.
    :type hedge_name: str
    :param version: Version identifier of the hedging strategy.
    :type version: int
    :return: pandas DataFrame containing the 'hedged_growth' and 'unhedged_growth' series,
             or None if the portfolio, strategy, or data is not found.
    :rtype: pandas.DataFrame or None
    """
    portfolio = session.query(PORTFOLIO).filter_by(portfolio_name=portfolio_name).first()
    if not portfolio:
        print(f"Portfolio '{portfolio_name}' not found.")
        return None

    strategy = session.query(HEDGING_STRATEGY).filter_by(hedge_name=hedge_name, version=version).first()
    if not strategy:
        print(f"Hedging strategy '{hedge_name}' or version '{version}' not found.")
        return None

    sim_ref = session.query(SIMULATION_REF).filter_by(
        hedge_id=strategy.hedge_id,
        portfolio_id=portfolio.portfolio_id
    ).first()
    if not sim_ref:
        print(f"No simulation found for portfolio '{portfolio_name}', hedge '{hedge_name}', and version '{version}'.")
        return None

    sim_data = session.query(SIMULATION_TS).filter_by(simulation_id=sim_ref.simulation_id).all()
    if not sim_data:
        print("No simulation time series data found.")
        return None

    df = orm_list_to_df(sim_data, date_index=True)[["hedged_growth", "unhedged_growth"]]
    return df
