from database.models import *
from database.session import session
import pandas as pd

def insert_simulation(portfolio_name: str, hedge_name: str, version: float, df: pd.DataFrame):
    """
    Insert simulation time series into the database for a given portfolio and hedging strategy.

    This function verifies that the specified portfolio exists (raising an error if not),
    creates or retrieves the hedging strategy and simulation reference, and bulk-inserts
    any new rows from the provided DataFrame into the SIMULATION_TS table.

    :param portfolio_name: Name of the portfolio.
    :type portfolio_name: str
    :param hedge_name: Name of the hedging strategy.
    :type hedge_name: str
    :param version: Version identifier of the hedging strategy.
    :type version: float
    :param df: DataFrame containing the simulation time series. Must have columns
               'date', 'hedged_growth', and 'unhedged_growth'; if 'date' is the index,
               it will be reset.
    :type df: pandas.DataFrame
    :raises ValueError: If required columns are missing or the portfolio does not exist.
    """
    # Ensure 'date' is not an index column
    if df.index.name == 'date':
        df = df.reset_index()

    # Check required columns
    required = {'date', 'hedged_growth', 'unhedged_growth'}
    if not required.issubset(df.columns):
        raise ValueError("DataFrame must contain 'date', 'hedged_growth', and 'unhedged_growth' columns.")

    # Convert 'date' to datetime.date
    df['date'] = pd.to_datetime(df['date']).dt.date

    with session:
        with session.begin():
            # Retrieve portfolio
            portfolio = session.query(PORTFOLIO).filter_by(portfolio_name=portfolio_name).first()
            if not portfolio:
                raise ValueError(f"Portfolio '{portfolio_name}' does not exist.")
            portfolio_id = portfolio.portfolio_id

            # Retrieve or create hedging strategy
            strategy = session.query(HEDGING_STRATEGY).filter_by(
                hedge_name=hedge_name, version=version
            ).first()
            if not strategy:
                strategy = HEDGING_STRATEGY(hedge_name=hedge_name, version=version)
                session.add(strategy)
                session.flush()
            hedge_id = strategy.hedge_id

            # Retrieve or create simulation reference
            sim_ref = session.query(SIMULATION_REF).filter_by(
                hedge_id=hedge_id, portfolio_id=portfolio_id
            ).first()
            if not sim_ref:
                sim_ref = SIMULATION_REF(hedge_id=hedge_id, portfolio_id=portfolio_id)
                session.add(sim_ref)
                session.flush()
            simulation_id = sim_ref.simulation_id

            # Find existing dates
            existing_dates = {
                d[0] for d in session.query(SIMULATION_TS.date)
                                      .filter_by(simulation_id=simulation_id)
                                      .all()
            }

            # Prepare new records
            new_records = [
                {
                    "simulation_id": simulation_id,
                    "date": row["date"],
                    "unhedged_growth": row["unhedged_growth"],
                    "hedged_growth": row["hedged_growth"]
                }
                for _, row in df.iterrows() if row["date"] not in existing_dates
            ]

            # Bulk insert
            if new_records:
                session.bulk_insert_mappings(SIMULATION_TS, new_records)
                print(f"Inserted {len(new_records)} new SIMULATION_TS records.")
            else:
                print("No new SIMULATION_TS entries to add.")
