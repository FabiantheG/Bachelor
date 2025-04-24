from datetime import date
from sqlalchemy import desc
from database.functions import *


from datetime import date
from sqlalchemy import desc
from database.functions import session, PORTFOLIO, HEDGING_STRATEGY, SIMULATION_REF, SIMULATION_TS

def insert_simulation(portfolio_name: str,
                      strategy_name: str,
                      unhedged_growth: list[float],
                      hedged_growth: list[float],
                      initial_version: float = 1.0) -> int | None:
    if len(unhedged_growth) != len(hedged_growth):
        raise ValueError("Lengths of unhedged_growth and hedged_growth must match")
    with session.begin():
        p = session.query(PORTFOLIO).filter_by(name=portfolio_name).first()
        if not p:
            print(f"Portfolio '{portfolio_name}' not found. Please create it first.")
            return None
        pid = p.portfolio_id

        latest = (session.query(HEDGING_STRATEGY)
                         .filter_by(name=strategy_name)
                         .order_by(desc(HEDGING_STRATEGY.version))
                         .first())
        new_version = round((latest.version + 0.1) if latest else initial_version, 1)

        strat = HEDGING_STRATEGY(name=strategy_name, version=new_version)
        session.add(strat); session.flush()

        sr = SIMULATION_REF(hedge_id=strat.hedge_id, portfolio_id=pid)
        session.add(sr); session.flush()

        today = date.today()
        for u, h in zip(unhedged_growth, hedged_growth):
            session.add(SIMULATION_TS(simulation_id=sr.simulation_id,date=today,unhedged_growth=u,hedged_growth=h))
        return sr.simulation_id















