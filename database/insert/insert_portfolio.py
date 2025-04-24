
from database.functions import *




def insert_portfolio(portfolio_name, investor_currency, asset_tickers, weights):
    """
    Create a Portfolio (or fetch an existing one) and link it to existing Assets with specified weights.
    If an asset ticker is not found, it only logs a warning and skips it.
    Does NOT update weights for already-linked assets.

    :param portfolio_name:    Name of the portfolio (string)
    :param investor_currency: Base currency of the investor (e.g. 'CHF')
    :param asset_tickers:     List of asset tickers (e.g. ['AAPL', 'MSFT'])
    :param weights:           List of weights for each asset (e.g. [0.4, 0.6])
    :return:                   portfolio_id (integer)
    """
    if len(asset_tickers) != len(weights):
        raise ValueError("asset_tickers and weights must have the same length")

    with session:
        with session.begin():
            # 1) Create or retrieve the Portfolio
            existing_portfolio = session.query(PORTFOLIO).filter_by(name=portfolio_name).first()
            if existing_portfolio:
                portfolio_id = existing_portfolio.portfolio_id
                print(f"Using existing portfolio '{portfolio_name}' (ID {portfolio_id})")
            else:
                new_portfolio = PORTFOLIO(
                    name=portfolio_name,
                    investor_cur=investor_currency
                )
                session.add(new_portfolio)
                session.flush()    # assigns portfolio_id
                portfolio_id = new_portfolio.portfolio_id
                print(f"Created new portfolio '{portfolio_name}' (ID {portfolio_id})")

            # 2) For each ticker & weight: only create association if not already present
            for ticker, weight in zip(asset_tickers, weights):
                # 2a) Check Asset existence
                asset = session.query(ASSET).filter_by(asset_ticker=ticker).first()
                if not asset:
                    print(f"Asset '{ticker}' not found, must be added to ASSETS table")
                    continue

                # 2b) Portfolio–Asset association: insert only if missing
                existing_association = (
                    session
                    .query(PORTFOLIO_ASSET_CONNECTION)
                    .filter_by(
                        portfolio_id=portfolio_id,
                        asset_ticker=ticker
                    )
                    .first()
                )
                if existing_association:
                    print(f"Association for '{ticker}' already exists")
                else:
                    new_association = PORTFOLIO_ASSET_CONNECTION(
                        portfolio_id=portfolio_id,
                        asset_ticker=ticker,
                        weight=weight
                    )
                    session.add(new_association)
                    print(f"Linked '{ticker}' to portfolio with weight {weight}")

            return portfolio_id





