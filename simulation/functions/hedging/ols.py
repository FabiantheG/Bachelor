

import pandas as pd
import statsmodels.api as sm
import numpy as np
from sympy.simplify.simplify import factor_sum




def ols(cur_list, hedge_logreturn, factors , window=60):


    # Gemeinsamer Index
    factors['dollar_shifted'] = factors['dollar'].shift(-1)
    factors['carry_shifted'] = factors['carry'].shift(-1)

    f = pd.concat([factors, hedge_logreturn], axis=1, join='inner')
    factors = f[['carry', 'dollar']]
    factors_shifted = f[['carry_shifted', 'dollar_shifted']]
    hedge_logreturn = f[cur_list]
    predictors = f[['ted','volatility','afd','commodity']]



    # Prepare output DataFrames
    hedge_ratio_df = pd.DataFrame(index=hedge_logreturn.index[window:-1], columns=cur_list)
    er_df = pd.DataFrame(index=hedge_logreturn.index[window:-1], columns=cur_list)
    hit_ratio_dict = {}  # Trefferquote speichern

    for cur in cur_list:
        hedge_cur = hedge_logreturn[cur] # take just the logreturn of this currency

        # count  - - -- - - -  - - --
        correct_count = 0
        total_count = 0
        # - - - -- - - - - -- - - - -

        for t in range(window, len(hedge_cur) - 1):

            hedge_window = hedge_cur.iloc[t - window:t] # take t-60 to t
            factor_window = factors.iloc[t - window:t] # take t-60 to t

            if t != 60:

                factor_shifted_window = factors_shifted.iloc[t - (window+1):t-1]
                predictor_window = predictors.iloc[t - (window+1):t-1]
            else:
                factor_shifted_window = factors_shifted.iloc[t - (window):t-1]
                predictor_window = predictors.iloc[t - (window):t-1]



            if hedge_window.isnull().any() or factor_window.isnull().any().any():
                continue

            # calculation of the carry and dollar betas
            factor_window = sm.add_constant(factor_window, has_constant='add')
            model = sm.OLS(hedge_window, factor_window).fit()
            beta = model.params

            # predict carry with predictors - - - - - -- - - - -- - - - - - - -
            predictor_window = sm.add_constant( predictor_window, has_constant='add')
            y_carry = factor_shifted_window[['carry_shifted']]


            model_carry = sm.OLS(y_carry,  predictor_window).fit()
            beta_carry = model_carry.params

            pred = sm.add_constant(predictors.iloc[t-1:t], has_constant='add')
            expected_carry = float(pred @ beta_carry)

            # predict dollar with predictors - - - - - - - - -- - - - - - -- -
            model_dollar = sm.OLS(factor_shifted_window['dollar_shifted'],  predictor_window).fit()
            beta_dollar = model_dollar.params

            # Aktuelle Faktoren (Zeitpunkt t)
            pred2 = sm.add_constant(predictors.iloc[t-1:t], has_constant='add')
            expected_dollar = float(pred2 @ beta_dollar)




            # Aktuelle Faktoren (Zeitpunkt t)

            X_t = sm.add_constant(factors.iloc[t:t+1], has_constant='add')

            X_t['carry'] = expected_carry
            X_t['dollar'] = expected_dollar

            expected_return = float(X_t @ beta)

            # Hedge Decision
            hedge_ratio = 1.0 if expected_return < 0 else 0
            current_date = hedge_cur.index[t]
            hedge_ratio_df.at[current_date, cur] = hedge_ratio
            er_df.at[current_date, cur] = expected_return

            # Evaluate correctness - - - - - - - - - -- - - - - - - - - - - -
            actual_return = hedge_cur.iloc[t]  # realisierter Forward Return bei t+1
            if (hedge_ratio == 1 and actual_return > 0) or (hedge_ratio == 0 and actual_return <= 0):
                correct_count += 1
            total_count += 1

            # - - - - - - - - - - - - - -  - -  -   - - - - - -- - - - - - - -- -  -

        # Trefferquote speichern
        hit_ratio = correct_count / total_count if total_count > 0 else np.nan
        hit_ratio_dict[cur] = hit_ratio

    #hedge_ratio_df = hedge_ratio_df.shift(1)  # shift decisions for timing

    return hedge_ratio_df.astype(float), er_df.astype(float), hit_ratio_dict


def ols_old(cur_list, df_fwd, carry_factor_df, dollar_factor_df, window=60):
    import statsmodels.api as sm
    import numpy as np
    import pandas as pd

    # Combine factor data
    factors = pd.concat([carry_factor_df, dollar_factor_df], axis=1)
    factors.columns = ['Carry', 'Dollar']

    # Shift forward returns to align with decision timing (t decision → return at t+1)
    #df_fwd_shifted = df_fwd.shift(-1)

    # Gemeinsamer Index
    f = pd.concat([factors, df_fwd_shifted], axis=1, join='inner')
    factors = f[['Carry', 'Dollar']]
    df_fwd_shifted = f[cur_list]

    # Prepare output DataFrames
    hedge_ratio_df = pd.DataFrame(index=df_fwd_shifted.index[window:-1], columns=cur_list)
    er_df = pd.DataFrame(index=df_fwd_shifted.index[window:-1], columns=cur_list)
    hit_ratio_dict = {}  # Trefferquote speichern

    for cur in cur_list:
        y = df_fwd_shifted[cur]
        correct_count = 0
        total_count = 0

        for t in range(window, len(y) - 1):
            y_window = y.iloc[t - window:t]
            X_window = factors.iloc[t - window:t]

            if y_window.isnull().any() or X_window.isnull().any().any():
                continue

            X_window = sm.add_constant(X_window, has_constant='add')
            model = sm.OLS(y_window, X_window).fit()
            beta = model.params

            # Aktuelle Faktoren (Zeitpunkt t)
            X_t = sm.add_constant(factors.iloc[t:t+1], has_constant='add')
            expected_return = float(X_t @ beta)

            # Hedge Decision
            hedge_ratio = 1.0 if expected_return < 0 else 0
            current_date = y.index[t]
            hedge_ratio_df.at[current_date, cur] = hedge_ratio
            er_df.at[current_date, cur] = expected_return

            # Evaluate correctness
            actual_return = y.iloc[t]  # realisierter Forward Return bei t+1
            if (hedge_ratio == 1 and actual_return > 0) or (hedge_ratio == 0 and actual_return <= 0):
                correct_count += 1
            total_count += 1

        # Trefferquote speichern
        hit_ratio = correct_count / total_count if total_count > 0 else np.nan
        hit_ratio_dict[cur] = hit_ratio

    hedge_ratio_df = hedge_ratio_df.shift(1)  # shift decisions for timing

    return hedge_ratio_df.astype(float), er_df.astype(float), hit_ratio_dict

def ols2(cur_list, df_fwd, carry_factor_df, dollar_factor_df, window=60):

    # Combine factor data
    factors = pd.concat([carry_factor_df, dollar_factor_df], axis=1)
    factors.columns = ['Carry', 'Dollar']


    # Shift forward returns to align with decision timing (t decision → return at t+1)
    df_fwd_shifted = df_fwd.shift(-1)


    f = pd.concat([factors, df_fwd_shifted], axis=1, join='inner')
    factors = f[['Carry', 'Dollar']]

    df_fwd_shifted = f[cur_list]

    # Prepare output DataFrame takes the first 60 (window) entries away
    hedge_ratio_df = pd.DataFrame(index=df_fwd_shifted.index[window:-1], columns=cur_list)
    er_df = pd.DataFrame(index=df_fwd_shifted.index[window:-1], columns=cur_list)

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
            hedge_ratio = 1.0 if expected_return < 0 else 0
            hedge_ratio_df.at[y.index[t], cur] = hedge_ratio
            er_df.at[y.index[t], cur] = expected_return

    hedge_ratio_df = hedge_ratio_df.shift(1) # because we get the reward one month before

    return hedge_ratio_df.astype(float), er_df.astype(float)