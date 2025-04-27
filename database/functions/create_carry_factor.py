from database.models import *
from database.session import session
from database.functions import *
import pandas as pd


def create_carry_factor():
    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK']
    spot_data = {}
    fwd_data = {}

    # 1. Load and resample spot and forward rates
    for quote in g10:
        spot = get_fx('USD', quote, duration='Spot').resample('M').last()
        fwd = get_fx('USD', quote, duration='1M').resample('M').last()

        spot_data[quote] = spot['rate']
        fwd_data[quote] = fwd['rate']

    spot_df = pd.concat(spot_data, axis=1)
    fwd_df = pd.concat(fwd_data, axis=1)

    # 2. Calculate carry premium: (Spot - Forward) / Spot
    carry_premium = (spot_df - fwd_df) / spot_df

    carry_factors = []

    # 3. Loop over months
    for date in carry_premium.index[:-1]:  # last month has no forward return
        cp = carry_premium.loc[date]

        # Rank currencies
        top3 = cp.nlargest(3).index
        bottom3 = cp.nsmallest(3).index

        # Calculate FX forward returns for next month
        next_month = carry_premium.index[carry_premium.index.get_loc(date) + 1]

        returns_next_month = {}

        for quote in g10:
            spot_today = spot_df.loc[date, quote]
            spot_next = spot_df.loc[next_month, quote]
            fwd_today = fwd_df.loc[date, quote]

            fx_return = (fwd_today / spot_next) - 1
            returns_next_month[quote] = fx_return

        returns_next_month = pd.Series(returns_next_month)

        # Carry factor = mean(long) - mean(short)
        carry_factor = returns_next_month[top3].mean() - returns_next_month[bottom3].mean()
        carry_factors.append((next_month, carry_factor))

    # 4. Return as DataFrame
    carry_df = pd.DataFrame(carry_factors, columns=['date', 'carry_factor']).set_index('date')

    return carry_df