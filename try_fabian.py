from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd
import numpy as np
import statsmodels.api as sm


portfolio_name = 'portfolio1'
x = get_portfolio_data(portfolio_name)





#metrics = metric_simulation(df)


#plot_simulation(df,save=True)

#insert_portfolio('portfolio1','CHF',['MSCI_UK','MSCI_USA'],[0.5,0.5])


dollar  = get_factor('dollar')
carry = get_factor('carry')

def compute_hedge_ratio_series(cur_list, df_fwd, carry_factor_df, dollar_factor_df, window=60):

    # Combine factor data
    factors = pd.concat([carry_factor_df, dollar_factor_df], axis=1)
    factors.columns = ['Carry', 'Dollar']


    # Shift forward returns to align with decision timing (t decision → return at t+1)
    df_fwd_shifted = df_fwd.shift(-1)
    factors = factors.loc[df_fwd_shifted.index]


    # Prepare output DataFrame takes the first 60 (window) entries away
    hedge_ratio_df = pd.DataFrame(index=df_fwd_shifted.index[window:-1], columns=cur_list)

    for cur in cur_list:
        y = df_fwd_shifted[cur]

        for t in range(window, len(y) - 1):
            y_window = y.iloc[t - window:t]
            X_window = factors.iloc[t - window:t]

            if y_window.isnull().any() or X_window.isnull().any().any():
                continue

            X_window = sm.add_constant(X_window, has_constant='add')
            model = sm.OLS(y_window, X_window).fit() #(Ordinary Least Squares, OLS)
            beta = model.params

            # Add constant also to X_t (aktuelle Faktoren bei Zeitpunkt t)
            X_t = sm.add_constant(factors.iloc[t:t+1], has_constant='add')
            expected_return = float(np.dot(X_t.values, beta.values))

            # Hedge if expected return < 0
            hedge_ratio = 1.0 if expected_return < 0 else 0.0
            hedge_ratio_df.at[y.index[t], cur] = hedge_ratio

    hedge_ratio_df = hedge_ratio_df.shift(1) # because we get the reward one month before

    return hedge_ratio_df.astype(float)

ratio_df = compute_hedge_ratio_series(x[1],x[3],carry,dollar)


df = simulate_portfolio(x[0],x[1],x[2],x[3],x[4],hedge_ratio = ratio_df)

#print(ratio_df)
plot_simulation(df)
metrics = metric_simulation(df)
latex  = format_latex_table(metrics)

