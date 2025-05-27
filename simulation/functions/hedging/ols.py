

import pandas as pd
import statsmodels.api as sm
import numpy as np

import pandas as pd
import statsmodels.api as sm
import numpy as np

def ols(cur_list, hedge_logreturn, factors, window=60, duration=1):
    # Gemeinsamer Index vorbereiten
    factors['dollar_shifted'] = factors['dollar'].shift(-1)
    factors['carry_shifted'] = factors['carry'].shift(-1)

    # Gemeinsames DataFrame für alle Inputs
    f = pd.concat([factors, hedge_logreturn], axis=1, join='inner')
    factors = f[['carry', 'dollar']]
    factors_shifted = f[['carry_shifted', 'dollar_shifted']]
    hedge_logreturn = f[cur_list]
    predictors = f[['ted', 'volatility', 'afd', 'commodity']]

    # Output-Container
    hedge_ratio_df = pd.DataFrame(index=hedge_logreturn.index[window:], columns=cur_list)
    er_df = pd.DataFrame(index=hedge_logreturn.index[window:], columns=cur_list)
    hit_ratio_dict = {}

    # Loop über alle Währungen
    for cur in cur_list:
        hedge_cur = hedge_logreturn[cur]
        correct_count = 0
        total_count = 0

        # Rolling Window manuell per while-loop
        t = window
        while t <= len(hedge_cur) - duration:
            hedge_window = hedge_cur.iloc[t - window:t]
            factor_window = factors.iloc[t - window:t]

            if t != window:
                factor_shifted_window = factors_shifted.iloc[t - (window + 1):t - 1]
                predictor_window = predictors.iloc[t - (window + 1):t - 1]
            else:
                factor_shifted_window = factors_shifted.iloc[t - window:t - 1]
                predictor_window = predictors.iloc[t - window:t - 1]

            if hedge_window.isnull().any() or factor_window.isnull().any().any():
                t += duration
                continue

            # 1. Hedge-Funktion: OLS für Carry + Dollar
            factor_window_const = sm.add_constant(factor_window, has_constant='add')
            model = sm.OLS(hedge_window, factor_window_const).fit()
            beta = model.params

            # 2. Carry-Vorhersage
            predictor_window_const = sm.add_constant(predictor_window, has_constant='add')
            y_carry = factor_shifted_window[['carry_shifted']]
            model_carry = sm.OLS(y_carry, predictor_window_const).fit()
            beta_carry = model_carry.params

            pred = sm.add_constant(predictors.iloc[t - 1:t], has_constant='add')
            expected_carry = float(pred @ beta_carry)

            # 3. Dollar-Vorhersage
            model_dollar = sm.OLS(factor_shifted_window['dollar_shifted'], predictor_window_const).fit()
            beta_dollar = model_dollar.params
            pred2 = sm.add_constant(predictors.iloc[t - 1:t], has_constant='add')
            expected_dollar = float(pred2 @ beta_dollar)

            # 4. Erwartete Returns und Hedge-Entscheidung für t+1, t+2, ..., t+duration
            for h in range(1, duration + 1):
                forecast_idx = t + h - 1
                if forecast_idx >= len(hedge_cur):
                    continue

                current_date = hedge_cur.index[forecast_idx]
                X_t = sm.add_constant(factors.iloc[t:t + 1], has_constant='add')
                X_t['carry'] = expected_carry
                X_t['dollar'] = expected_dollar

                expected_return = float(X_t @ beta)
                hedge_ratio = 1.0 if expected_return < 0 else 0

                hedge_ratio_df.at[current_date, cur] = hedge_ratio
                er_df.at[current_date, cur] = expected_return

                # Trefferquote berechnen
                actual_return = hedge_cur.iloc[forecast_idx]
                if (hedge_ratio == 1 and actual_return > 0) or (hedge_ratio == 0 and actual_return <= 0):
                    correct_count += 1
                total_count += 1

            t += duration  # Rolling-Window-Sprung entsprechend der Dauer

        hit_ratio = correct_count / total_count if total_count > 0 else np.nan
        hit_ratio_dict[cur] = hit_ratio

    return hedge_ratio_df.astype(float), er_df.astype(float), hit_ratio_dict

