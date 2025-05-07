

def metric_simulation(df_growth, risk_free_rate_annual=0):
    """
    Evaluate performance metrics for each strategy given a DataFrame of cumulative growth.

    This function assumes the input DataFrame contains cumulative growth data (e.g., starting at 1).
    It first resamples the data to annual frequency, computes log returns, adjusts the risk-free rate,
    and then calculates:
        - Mean return
        - Standard deviation
        - Sharpe ratio
        - Sortino ratio
        - Certainty equivalent (CEQ)
        - Skewness
        - Kurtosis
        - Maximum drawdown

    :param df_growth: DataFrame of cumulative growth time series (each column is a strategy).
    :type df_growth: pandas.DataFrame
    :param risk_free_rate_annual: Annualized risk-free rate.
    :type risk_free_rate_annual: float
    :return: DataFrame with performance metrics for each strategy.
    :rtype: pandas.DataFrame
    """
    import numpy as np
    import pandas as pd

    # Resample to annual frequency (year-end)
    df_annual = df_growth.resample('A').last()

    # Compute log returns from annual data
    returns = np.log(df_annual / df_annual.shift(1)).dropna()

    # Risk-free rate remains annual in this case
    risk_free_rate_log = np.log(1 + risk_free_rate_annual)

    def max_drawdown(series):
        cumulative = series.cumsum()
        cumulative_max = cumulative.cummax()
        drawdown = cumulative - cumulative_max
        return drawdown.min()

    metrics = {}

    for strategy in returns.columns:
        strategy_ret = returns[strategy].dropna()
        mean_logreturn = strategy_ret.mean()
        std_return = strategy_ret.std()
        negative_ret = strategy_ret[strategy_ret < 0]
        geometric_return = np.exp(mean_logreturn) - 1

        sharpe_ratio = (mean_logreturn - risk_free_rate_log) / std_return if std_return > 0 else np.nan
        sortino_ratio = ((mean_logreturn - risk_free_rate_log) / negative_ret.std()) if len(negative_ret) > 0 else np.nan
        ceq = mean_logreturn - 0.5 * (std_return ** 2)
        skew = strategy_ret.skew()
        kurtosis = strategy_ret.kurt()
        mdd = max_drawdown(strategy_ret)

        metrics[strategy] = {
            'mean_return': geometric_return,
            'std_return': std_return,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'ceq': ceq,
            'skew': skew,
            'kurtosis': kurtosis,
            'max_drawdown': mdd
        }

    return pd.DataFrame(metrics)


