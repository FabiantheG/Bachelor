from database.models import *
from database.session import session
from database.functions import *
import pandas as pd

def create_carry_factor(base_currency='CHF'):
    """
    Constructs a time series of the global carry factor from the perspective of a given base currency.

    The carry factor is computed as the return difference between a portfolio long in the top 3
    carry currencies and short in the bottom 3 carry currencies, where carry is defined as the
    (Spot - Forward) / Spot premium for 1-month FX contracts.

    Parameters:
    -----------
    base_currency : str
        The investor's home currency (default is 'CHF').

    Returns:
    --------
    carry_df : pd.DataFrame
        A DataFrame with monthly dates as index and a single column 'carry_factor' containing the
        carry factor return at each time step.
    """

    # G10 currencies (excluding base_currency)
    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK', 'USD']
    g10 = [curr for curr in g10 if curr != base_currency]

    spot_data = {}
    fwd_data = {}

    # 1. Load and resample spot and forward rates relative to the base currency
    for quote in g10:
        spot = get_fx(base_currency, quote, duration='Spot').resample('M').last()
        fwd = get_fx(base_currency, quote, duration='1M').resample('M').last()

        spot_data[quote] = spot['rate']
        fwd_data[quote] = fwd['rate']

    # Combine into DataFrames
    spot_df = pd.concat(spot_data, axis=1)
    fwd_df = pd.concat(fwd_data, axis=1)

    # 2. Calculate carry premium = (Spot - Forward) / Spot
    carry_premium = (spot_df - fwd_df) / spot_df

    carry_factors = []

    # 3. Loop over all months except the last one (since next-month spot is required)
    for date in carry_premium.index[:-1]:
        cp = carry_premium.loc[date]

        # Select top 3 and bottom 3 carry currencies
        top3 = cp.nlargest(3).index
        bottom3 = cp.nsmallest(3).index

        # Get the date of the next month
        next_month = carry_premium.index[carry_premium.index.get_loc(date) + 1]

        # Calculate next-month FX forward returns for each currency
        returns_next_month = {}

        for quote in cp.index:
            spot_today = spot_df.loc[date, quote]
            spot_next = spot_df.loc[next_month, quote]
            fwd_today = fwd_df.loc[date, quote]

            # FX return using 1-month forward contract
            fx_return = (fwd_today / spot_next) - 1
            returns_next_month[quote] = fx_return

        returns_next_month = pd.Series(returns_next_month)

        # 4. Carry factor = average return of long top 3 minus short bottom 3
        carry_factor = returns_next_month[top3].mean() - returns_next_month[bottom3].mean()
        carry_factors.append((next_month, carry_factor))

    # 5. Return as DataFrame
    carry_df = pd.DataFrame(carry_factors, columns=['date', 'carry_factor']).set_index('date')

    return carry_df