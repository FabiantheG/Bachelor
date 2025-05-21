



def xgboost(cur_list, hedge_logreturn, factors, factor_names, base = 'CHF',window=60):
    import statsmodels.api as sm
    import numpy as np
    import pandas as pd
    from xgboost import XGBRegressor
    from sklearn.linear_model import LinearRegression



    # Shift forward returns to align with decision timing (t decision → return at t+1)
    hedge_logreturn_shifted = hedge_logreturn.shift(-1)

    # columns of the factors df
    factors_column = factors.columns

    # Gemeinsamer Index
    f = pd.concat([factors, hedge_logreturn_shifted], axis=1, join='inner')
    factors = f[factors_column]
    hedge_logreturn_shifted = f[cur_list]

    # Prepare output DataFrames
    hedge_ratio_df = pd.DataFrame(index=hedge_logreturn_shifted.index[window:-1], columns=cur_list)
    er_df = pd.DataFrame(index=hedge_logreturn_shifted.index[window:-1], columns=cur_list)
    hit_ratio_dict = {}  # Trefferquote speichern

    step = 0
    for cur in cur_list:
        y = hedge_logreturn_shifted[cur] # y is the result that we should have
        correct_count = 0
        total_count = 0
        step += 1 # step to show the process


        # take just the factors of the current currency - - - - - - - - -- - - -
        factor_names_current = []

        if 'dollar' in factor_names:
            factor_names_current.append('dollar')

        if 'carry' in factor_names:
            factor_names_current.append('carry')

        if 'volatility' in factor_names:
            factor_names_current.append('volatility'+cur + base)

        factors_current = factors[factor_names_current]

        #  - - - - -- - --  -- - - - - -- - -- - - - - - -- - - - - -- - - - -- - -


        for t in range(window, len(y) - 1):
            y_window = y.iloc[t - window:t]
            X_window = factors_current.iloc[t - window:t]

            if y_window.isnull().any() or X_window.isnull().any().any():
                continue

            model = XGBRegressor(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                verbosity=0  # zwischenausgaben
            )

            model.fit(X_window, y_window)
            expected_return = float(model.predict(factors_current.iloc[t:t + 1])[0])

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

        print(str(step) + '/' + str(len(cur_list))) # to know how far the simulation is





    hedge_ratio_df = hedge_ratio_df.shift(1)  # shift decisions for timing

    return hedge_ratio_df.astype(float), er_df.astype(float), hit_ratio_dict