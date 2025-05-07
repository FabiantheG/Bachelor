


def format_latex_table(df_metrics, caption="Performance Metrics for the FX portfolio", label="tab:metrics"):
    """
    Format a DataFrame of performance metrics into a LaTeX table.

    :param df_metrics: DataFrame with metrics as rows and strategies as columns.
    :param caption: Caption text for the LaTeX table.
    :param label: Label for the LaTeX table for referencing.
    :return: LaTeX code as string.
    """
    df = df_metrics.copy()

    # Format % where sinnvoll (mean, std, ceq, mdd)
    percent_rows = ['mean_return', 'std_return', 'ceq', 'max_drawdown']
    renamed_rows = {
        'mean_return': 'mean (\\%)',
        'std_return': 'std (\\%)',
        'sharpe_ratio': 'Sharpe Ratio',
        'sortino_ratio': 'Sortino Ratio',
        'skew': 'Skew',
        'kurtosis': 'Kurtosis',
        'max_drawdown': 'MDD (\\%)',
        'ceq': 'CEQ (\\%)'
    }

    formatted = df.copy()
    for row in df.index:
        if row in percent_rows:
            formatted.loc[row] = df.loc[row] * 100
        formatted.loc[row] = formatted.loc[row].map(lambda x: f"{x:.2f}")

    # Build LaTeX string
    latex = "\\begin{table}[H]\n\\centering\n"
    latex += f"\\begin{{tabular}}{{l{'c' * len(df.columns)}}}\n"
    latex += "\\toprule\n"
    latex += "Metric & " + " & ".join(df.columns) + " \\\\\n"
    latex += "\\midrule\n"

    for row in df.index:
        name = renamed_rows.get(row, row)
        latex += f"{name} & " + " & ".join(formatted.loc[row]) + " \\\\\n"

    latex += "\\bottomrule\n"
    latex += "\\end{tabular}\n"
    latex += f"\\caption{{{caption}}}\n"
    latex += f"\\label{{{label}}}\n"
    latex += "\\end{table}\n"

    return latex