import numpy as np
import pandas as pd
import time
from xgboost import XGBRegressor
import shap

def xgboost(cur_list, hedge_logreturn, factors, window=60, duration=1, xgb_factor = None,xgb_predictors = None):

    # parameter
    if xgb_factor is None:
        xgb_factor = {
            'n_estimators': 50,
            'max_depth': 3
        }

    if xgb_predictors is None:
        xgb_predictors = {
            'n_estimators': 50,
            'max_depth': 3
        }


    # Shift target factors one step forward
    factors['dollar_shifted'] = factors['dollar'].shift(-1)
    factors['carry_shifted'] = factors['carry'].shift(-1)

    # Join everything into one aligned DataFrame
    f = pd.concat([factors, hedge_logreturn], axis=1, join='inner')
    factors = f[['carry', 'dollar']]
    factors_shifted = f[['carry_shifted', 'dollar_shifted']]
    hedge_logreturn = f[cur_list]
    predictors = f[['ted', 'volatility', 'afd', 'commodity']]

    # Outputs
    hedge_ratio_df = pd.DataFrame(index=hedge_logreturn.index[window:], columns=cur_list)
    predicted_hedgereturns = pd.DataFrame(index=hedge_logreturn.index[window:], columns=cur_list)
    predicted_carry = pd.DataFrame(index=hedge_logreturn.index[window:], columns=['carry'])
    predicted_dollar = pd.DataFrame(index=hedge_logreturn.index[window:], columns=['dollar'])

    hit_ratio_dict = {}



    length = len(cur_list)
    period = 0

    for cur in cur_list:
        start_time = time.time()
        hedge_cur = hedge_logreturn[cur]
        correct_count = 0
        total_count = 0

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

            # Model 1: Return = f(carry, dollar)
            model_return = XGBRegressor(**xgb_factor)
            model_return.fit(factor_window, hedge_window)


            # Model 2: Carry = f(predictors)
            model_carry = XGBRegressor(**xgb_predictors)
            model_carry.fit(predictor_window, factor_shifted_window['carry_shifted'])
            x_pred = predictors.iloc[t - 1:t]
            expected_carry = model_carry.predict(x_pred)[0]


            # Model 3: Dollar = f(predictors)
            model_dollar = XGBRegressor(**xgb_predictors)
            model_dollar.fit(predictor_window, factor_shifted_window['dollar_shifted'])
            expected_dollar = model_dollar.predict(x_pred)[0]


            for h in range(1, duration + 1):
                forecast_idx = t + h - 1
                if forecast_idx >= len(hedge_cur):
                    continue

                current_date = hedge_cur.index[forecast_idx]
                input_features = pd.DataFrame({'carry': [expected_carry], 'dollar': [expected_dollar]})
                expected_return = model_return.predict(input_features)[0]
                hedge_ratio = 1.0 if expected_return < 0 else 0

                hedge_ratio_df.at[current_date, cur] = hedge_ratio
                predicted_hedgereturns.at[current_date, cur] = expected_return


                predicted_carry.at[current_date,'carry'] = expected_carry
                predicted_dollar.at[current_date,'dollar'] = expected_dollar


                # Evaluate hit or miss
                actual_return = hedge_cur.iloc[forecast_idx]
                if (hedge_ratio == 1 and actual_return > 0) or (hedge_ratio == 0 and actual_return <= 0):
                    correct_count += 1
                total_count += 1

            t += duration

        hit_ratio = correct_count / total_count if total_count > 0 else np.nan
        hit_ratio_dict[cur] = hit_ratio

        period += 1
        time_left = round((time.time() - start_time) * (length - period), 2)
        print(f"{period}/{length} — {time_left} seconds left")

    return hedge_ratio_df.astype(float), predicted_hedgereturns.astype(float), hit_ratio_dict