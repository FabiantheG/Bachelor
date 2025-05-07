from database.functions import *


def insert_portfolio(portfolio_name, investor_currency, asset_tickers, weights):
    """
    Create or retrieve a portfolio and link it to existing assets with specified weights.

    If a portfolio with the given name exists, it is reused; otherwise a new one is created.
    For each ticker in `asset_tickers`, an association is created with the given weight
    unless the asset is missing or already linked.

    :param portfolio_name:    Name of the portfolio.
    :type portfolio_name:     str
    :param investor_currency: Base currency of the investor (e.g. 'CHF').
    :type investor_currency:  str
    :param asset_tickers:     List of asset tickers to include (e.g. ['AAPL', 'MSFT']).
    :type asset_tickers:      list of str
    :param weights:           List of weights corresponding to each ticker (e.g. [0.4, 0.6]).
    :type weights:            list of float
    :raises ValueError:       If `asset_tickers` and `weights` have different lengths.
    :return:                  The portfolio_id of the created or retrieved portfolio.
    :rtype:                   int
    """
    if len(asset_tickers) != len(weights):
        raise ValueError("asset_tickers and weights must have the same length")

    with session:

            # 1) Create or retrieve the Portfolio
            existing_portfolio = session.query(PORTFOLIO).filter_by(portfolio_name=portfolio_name).first()
            if existing_portfolio:
                portfolio_id = existing_portfolio.portfolio_id
                print(f"Using existing portfolio '{portfolio_name}' (ID {portfolio_id})")
            else:
                new_portfolio = PORTFOLIO(
                    portfolio_name=portfolio_name,
                    investor_cur=investor_currency
                )
                session.add(new_portfolio)
                session.flush()    # assigns portfolio_id
                portfolio_id = new_portfolio.portfolio_id
                print(f"Created new portfolio '{portfolio_name}' (ID {portfolio_id})")

            # 2) Link each asset & weight if not already present
            for ticker, weight in zip(asset_tickers, weights):
                asset = session.query(ASSET).filter_by(asset_ticker=ticker).first()
                if not asset:
                    print(f"Asset '{ticker}' not found, must be added to ASSETS table")
                    continue

                existing_assoc = (
                    session.query(PORTFOLIO_ASSET_CONNECTION)
                           .filter_by(portfolio_id=portfolio_id, asset_ticker=ticker)
                           .first()
                )
                if existing_assoc:
                    print(f"Association for '{ticker}' already exists")
                else:
                    new_assoc = PORTFOLIO_ASSET_CONNECTION(
                        portfolio_id=portfolio_id,
                        asset_ticker=ticker,
                        weight=weight
                    )
                    session.add(new_assoc)
                    print(f"Linked '{ticker}' to portfolio with weight {weight}")

            session.commit()
