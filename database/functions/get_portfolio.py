from database.models import *
from database.session import session
from database.functions.orm_list_to_df import *



def get_portfolio(portfolio_name):
    portfolio_id = session.query(PORTFOLIO.portfolio_id).filter_by(name=portfolio_name).first()


    if not portfolio_id:
        print("Portfolio not found")
        return None
    else:
        new = session.query(PORTFOLIO_ASSET_CONNECTION).filter_by(portfolio_id=portfolio_id[0]).all()
        df = orm_list_to_df(new, date_index = False)
        df = df.drop(columns=["portfolio_id"])
    return df



