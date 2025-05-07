import matplotlib.pyplot as plt

import pandas as pd




def plot_simulation(df, title='Growth', save=False, plot=True):
    """
    Plot and optionally save the growth of a portfolio over time.

    This function visualizes the time series of portfolio growth under different strategies or scenarios.
    It supports showing the plot directly or saving it as a PNG file.

    :param df: DataFrame with time series data to be plotted (e.g., local, unhedged, and hedged growth).
    :type df: pandas.DataFrame
    :param title: Title of the plot and filename (if saved).
    :type title: str
    :param save: If True, saves the plot as a PNG file in the 'pictures/' directory.
    :type save: bool
    :param plot: If True, displays the plot interactively.
    :type plot: bool
    :return: None
    """

    import matplotlib.pyplot as plt

    plt.style.use('seaborn-v0_8-darkgrid')
    plt.figure(figsize=(12, 6))

    for column in df:
        df[column].plot(label=column)  # Plot each column separately

    plt.legend(
        loc='upper left',
        bbox_to_anchor=(0.01, 0.99),
        frameon=True,
        fancybox=True,
        edgecolor='black'
    )
    plt.title(title)
    plt.xlabel('Time')
    plt.ylabel('Growth')

    if save:
        plt.savefig('pictures/' + title + '.png')  # Save first, then show

    if plot:
        plt.show()
