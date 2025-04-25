from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *

def get_portfolio(portfolio_name):
    """
    Load portfolio composition as a pandas DataFrame.

    Queries the database for the portfolio matching the given name,
    retrieves all asset connections for that portfolio, converts the ORM result
    into a pandas DataFrame (without using the date index), and removes the
    'portfolio_id' column.

    :param portfolio_name: Name of the portfolio to retrieve.
    :type portfolio_name: str
    :return: pandas DataFrame containing the portfolio's asset connections,
             or None if no portfolio is found.
    :rtype: pandas.DataFrame or None
    """
    portfolio_id = session.query(PORTFOLIO.portfolio_id) \
                          .filter_by(portfolio_name=portfolio_name) \
                          .first()

    if not portfolio_id:
        print("Portfolio not found")
        return None

    new = session.query(PORTFOLIO_ASSET_CONNECTION) \
                 .filter_by(portfolio_id=portfolio_id[0]) \
                 .all()
    df = orm_list_to_df(new, date_index=False)
    df = df.drop(columns=["portfolio_id"])

    return df
