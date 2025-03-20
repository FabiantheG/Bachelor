import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf
import mplfinance as mpf


def intra_today(data):  # macht aus 2024-04-04-339281291 -> 2024-04-04 nur das Datum bleibt
    dataframe = data.copy()
    dataframe.reset_index(inplace=True)
    dataframe = dataframe.copy()

    dataframe['Date'] = pd.to_datetime(dataframe['Date']).dt.date
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])
    return dataframe


def switch_fx(data):  # wechselt die spot und Forwardpreise z.B. CHFUSD -> USDCHF

    dataframe = data.copy()
    fwd = 1 / (dataframe['spot'] + dataframe['px'] / 10000)
    dataframe['spot'] = 1 / dataframe['spot']
    dataframe['px'] = (fwd - dataframe['spot']) * 10000

    return dataframe


def get_asset(ticker, start="2022-08-05", end="2024-11-04"):  # yahoo finance Finanzzeitreihe des Kurses
    asset = yf.Ticker(ticker)
    asset = asset.history(start=start, end=end, interval="1d")  # m = minute, d = day, mo = month
    asset = intra_today(asset)
    return asset


def get_spot(base, quote, start="2022-08-05", end="2024-10-15"):  # Yahoo Spotpreise eines Währungspaars
    ticker = base + quote + "=X"
    yahoo = yf.Ticker(ticker)
    spot = yahoo.history(start=start, end=end, interval="1d")
    spot = intra_today(spot)
    # change name of the column close to spot
    spot.rename(columns={'Close': 'spot'}, inplace=True)
    return spot


def merge_data(dataframe1, dataframe2, on='Date'):
    df = pd.merge(dataframe1, dataframe2, how='inner', on='Date')
    return df


def hedge(fx_dataframe, asset_dataframe, quote, base, hedge_ratio=1, plot=False):
    """
    Diese Funktion berechnet die gehedgten und ungehedgten Renditen eines Anlageportfolios und visualisiert optional das
    Wachstumsverhalten des Portfolios mit und ohne Währungssicherung (Hedging).

    Parameter:
    - fx_dataframe (pd.DataFrame): DataFrame mit den Wechselkursdaten, einschließlich der Spalten 'Date', 'spot' und 'px'.
    - asset_dataframe (pd.DataFrame): DataFrame mit den Asset-Daten, einschließlich der Spalten 'Date' und 'Close'.
    - hedge_ratio (float): Das Verhältnis der Absicherung. Standardmäßig 1 (vollständig abgesichert).
    - plot (bool): Wenn True, wird ein Diagramm der Wachstumsverläufe angezeigt.

    Ablauf:
    1. Bereinigung der Daten:
        - Erstellen von Kopien der DataFrames, um die Originaldaten unverändert zu lassen.
        - Filtern der relevanten Spalten in beiden DataFrames und Konvertieren des 'Date'-Formats.

    2. Zusammenführen der Asset- und Wechselkursdaten:
        - Die DataFrames werden basierend auf dem Datum zusammengeführt, um eine gemeinsame Grundlage für die Berechnungen zu schaffen.

    3. Berechnung des Terminkurses und des ungehedgten CHF-Betrags:
        - Der Terminkurs ('forward') wird berechnet, indem die Forward Points ('px') zum Kassakurs ('spot') addiert werden.
        - Der 'unhedgedCHF'-Wert stellt den ungehedgten CHF-Wert der Anlage dar.

    4. Berechnung der Renditen:
        - Berechnung der logarithmischen Renditen für den Forward-Kurs, den Kassakurs und das Hedging.
        - Die Hedging-Rendite ('hedge_logreturn') wird berechnet, indem die Differenz zwischen der Forward-Rendite und der Kassakurs-Rendite mit dem Hedge-Ratio multipliziert wird.

    5. Berechnung der log-Renditen und des Wachstums:
        - Die lokalen log-Renditen der Anlage werden berechnet.
        - Die vollständig abgesicherten log-Renditen (inkl. Hedge) und die ungehedgten log-Renditen werden berechnet und kumulativ summiert.
        - Die kumulierten log-Renditen werden exponentiell rücktransformiert, um den Wachstumsverlauf zu erhalten.

    6. Optional: Plotten der Wachstumsverläufe:
        - Wenn plot=True ist, wird ein Diagramm erstellt, das die gehedgten, lokalen und ungehedgten Wachstumsverläufe vergleicht.

    Rückgabewert:
    pd.DataFrame: Ein DataFrame mit den berechneten Werten für die gehedgten und ungehedgten Wachstumsverläufe sowie die kumulierten log-Renditen.

    Beispiel:
    >>> hedge(fx_dataframe, asset_dataframe, hedge_ratio=0.8, plot=True)
    """
    # Erstellen einer Kopie, um Änderungen zu vermeiden
    fx_dataframe = fx_dataframe.copy()
    asset_dataframe = asset_dataframe.copy()

    # clean asset dataframe and fx dataframe
    asset_dataframe = asset_dataframe.filter(regex='Close|Date')  # clean asset dataframe
    fx_dataframe = fx_dataframe.filter(regex='px|Date|spot')  # clean fx dataframe

    fx_dataframe['Date'] = pd.to_datetime(fx_dataframe['Date'])
    asset_dataframe.reset_index(inplace=True)
    asset_dataframe = asset_dataframe.copy()
    asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date']).dt.date
    asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date'])

    # merge asset and fx dataframes and create new dataframe df
    df = pd.merge(asset_dataframe, fx_dataframe, how='inner', on='Date')

    # calculate the forward price
    if base == 'JPY' or quote == 'JPY':
        if base == 'EUR' or quote == 'EUR':
            df['forward'] = df['spot'] - df['px'] / 1000
        df['forward'] = df['spot'] - df['px'] / 100
    else:
        df['forward'] = df['spot'] + df['px'] / 10000
    df['unhedgedCHF'] = df['Close'] * df['spot']

    df['unhedgedCHF'] = df['Close'] * df['spot']

    # FX forward return berechnen (logarithmisch)
    df["fwd_logreturn"] = np.log(df["forward"].shift(1)) - np.log(df["spot"].shift(1))

    # FX return berechnen (logarithmisch)
    df["fx_logreturn"] = np.log(df["spot"]) - np.log(df["spot"].shift(1))

    # calculate the (negative) hedge return (hedge_cost - currency return) multiplied by the hedge ratio
    df["hedge_logreturn"] = (df["fwd_logreturn"] - df["fx_logreturn"]) * hedge_ratio

    # returns berechnen
    # df['returns'] = df['CloseUSD'].pct_change()

    # log returns berechnen
    df['local_logreturns'] = np.log(df['Close']).diff()
    df['total_hedged_logreturns'] = df['local_logreturns'] + df['fx_logreturn'] + df['hedge_logreturn']
    # df['unhedged_logreturns'] = np.log(df['unhedgedCHF']).diff()

    df['unhedged_logreturns'] = df["fx_logreturn"] + df['local_logreturns']

    # CUMSUM
    df['total_hedgedcum_logreturns'] = df['total_hedged_logreturns'].cumsum()
    df['localcum_logreturns'] = df['local_logreturns'].cumsum()
    df['unhedgedcum_logreturns'] = df['unhedged_logreturns'].cumsum()

    # Rücktransformation
    df['hedged_growth'] = np.exp(df['total_hedgedcum_logreturns']).fillna(1)
    df['local_growth'] = np.exp(df['localcum_logreturns']).fillna(1)
    df['unhedged_growth'] = np.exp(df['unhedgedcum_logreturns']).fillna(1)

    if plot == True:
        # Plotten der drei Wachstumsreihen mit Labels
        df['hedged_growth'].plot(label='Hedged Growth')
        df['local_growth'].plot(label='Local Growth')
        df['unhedged_growth'].plot(label='Unhedged Growth')

        # Hinzufügen der Legende
        plt.legend()

        # Titel und Achsenbeschriftungen für bessere Übersicht
        plt.title('Wachstumsverläufe: Hedged, Local und Unhedged')
        plt.xlabel('Zeit')
        plt.ylabel('Wachstumsfaktor')

        # Zeige das Diagramm
        plt.show()

    return df


def plot_hedge(df, titel='Wachstumsverläufe: Hedged, Local und Unhedged'):
    plt.style.use('seaborn-darkgrid')
    plt.figure(figsize=(12, 6))
    # Plotten der drei Wachstumsreihen mit Labels
    df['hedged_growth'].plot(label='hedged growth')
    df['local_growth'].plot(label='local growth')
    df['unhedged_growth'].plot(label='unhedged growth')

    # Hinzufügen der Legende
    plt.legend(
        loc='upper left',  # Legend location
        bbox_to_anchor=(0.01, 0.99),  # Position relative to the plot
        frameon=True,  # Turn on the frame
        fancybox=True,  # Rounded corners for the box
        edgecolor='black'  # Color of the box's border
    )

    # Titel und Achsenbeschriftungen für bessere Übersicht
    plt.title(titel)
    plt.xlabel('Time')
    plt.ylabel('Growth')

    # Zeige das Diagramm
    plt.show()


def hedge_momentum(fx_dataframe, asset_dataframe, quote, base='CHF', hedge_ratio=1, plot=False, num_horizon=90):
    # Erstellen einer Kopie, um Änderungen zu vermeiden
    fx_dataframe = fx_dataframe.copy()
    asset_dataframe = asset_dataframe.copy()

    fx_dataframe['momentum'] = fx_dataframe['spot'] / fx_dataframe['spot'].shift(num_horizon)

    # clean asset dataframe and fx dataframe
    asset_dataframe = asset_dataframe.filter(regex='Close|Date')  # clean asset dataframe
    fx_dataframe = fx_dataframe.filter(regex='px|Date|spot|momentum')  # clean fx dataframe

    fx_dataframe['Date'] = pd.to_datetime(fx_dataframe['Date'])
    asset_dataframe.reset_index(inplace=True)
    asset_dataframe = asset_dataframe.copy()
    # asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date']).dt.date
    asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date'])

    # merge asset and fx dataframes and create new dataframe df
    df = pd.merge(asset_dataframe, fx_dataframe, how='inner', on='Date')

    # calculate the forward price
    if base == 'JPY' or quote == 'JPY':
        if base == 'EUR' or quote == 'EUR':
            df['forward'] = df['spot'] - df['px'] / 1000
        df['forward'] = df['spot'] - df['px'] / 100
    else:
        df['forward'] = df['spot'] + df['px'] / 10000
    df['unhedgedCHF'] = df['Close'] * df['spot']

    # FX forward return berechnen (logarithmisch)
    # df["fwd_logreturn"] = np.log(df["forward"].shift(1)) - np.log( df["spot"].shift(1))
    df["fwd_logreturn"] = np.log(df["forward"]) - np.log(df["spot"])

    # FX return berechnen (logarithmisch)
    df["fx_logreturn"] = np.log(df["spot"]) - np.log(df["spot"].shift(1))

    # calculate the (negative) hedge return (hedge_cost - currency return) multiplied by the hedge ratio
    df["hedge_logreturn"] = (df["fwd_logreturn"] - df["fx_logreturn"]) * hedge_ratio

    # returns berechnen
    # df['returns'] = df['CloseUSD'].pct_change()

    # log returns berechnen
    df['local_logreturns'] = np.log(df['Close']).diff()

    # change

    df['momentum'] = df['momentum'].shift(1).fillna(0)

    df['total_hedged_logreturns'] = np.where(df['momentum'] < 1,
                                             df['local_logreturns'] + df['fx_logreturn'] + df['hedge_logreturn'],
                                             df['local_logreturns'] + df['fx_logreturn'])

    df['unhedged_logreturns'] = df["fx_logreturn"] + df['local_logreturns']

    # CUMSUM
    df['total_hedgedcum_logreturns'] = df['total_hedged_logreturns'].cumsum()
    df['localcum_logreturns'] = df['local_logreturns'].cumsum()
    df['unhedgedcum_logreturns'] = df['unhedged_logreturns'].cumsum()

    # Rücktransformation
    df['hedged_growth'] = np.exp(df['total_hedgedcum_logreturns']).fillna(1)
    df['local_growth'] = np.exp(df['localcum_logreturns']).fillna(1)
    df['unhedged_growth'] = np.exp(df['unhedgedcum_logreturns']).fillna(1)

    if plot == True:
        # Plotten der drei Wachstumsreihen mit Labels
        df['hedged_growth'].plot(label='Hedged Growth')
        df['local_growth'].plot(label='Local Growth')
        df['unhedged_growth'].plot(label='Unhedged Growth')

        # Hinzufügen der Legende
        plt.legend()

        # Titel und Achsenbeschriftungen für bessere Übersicht
        plt.title('Wachstumsverläufe: Hedged, Local und Unhedged')
        plt.xlabel('Zeit')
        plt.ylabel('Wachstumsfaktor')

        # Zeige das Diagramm
        plt.show()

    return df


def hedge_carry(deposit_rate, fx_dataframe, asset_dataframe, quote, base='CHF', hedge_ratio=1, plot=False):
    '''
    # so kriegt man die deposit rate Daten
    deposit_rate = pd.read_csv('Daten/1m_deposit_rates.csv',sep = ';')
    deposit_rate['Date'] = pd.to_datetime(deposit_rate['Date'],dayfirst=True)
    '''
    # Erstellen einer Kopie, um Änderungen zu vermeiden
    fx_dataframe = fx_dataframe.copy()
    asset_dataframe = asset_dataframe.copy()

    deposit_rate = deposit_rate.copy()

    deposit_rate['carry'] = deposit_rate[base] - deposit_rate[quote]

    # clean asset dataframe and fx dataframe
    asset_dataframe = asset_dataframe.filter(regex='Close|Date')  # clean asset dataframe
    fx_dataframe = fx_dataframe.filter(regex='px|Date|spot')  # clean fx dataframe
    deposit_rate = deposit_rate.filter(regex='Date|carry|' + quote + '|' + base)  # clean deposit rate dataframe

    deposit_rate['Date'] = pd.to_datetime(deposit_rate['Date'])

    fx_dataframe['Date'] = pd.to_datetime(fx_dataframe['Date'])
    asset_dataframe.reset_index(inplace=True)
    asset_dataframe = asset_dataframe.copy()
    asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date']).dt.date
    asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date'])

    # merge asset and fx dataframes and create new dataframe df
    df = pd.merge(asset_dataframe, fx_dataframe, how='inner', on='Date')

    df = pd.merge(df, deposit_rate, how='inner', on='Date')

    if base == 'JPY' or quote == 'JPY':
        if base == 'EUR' or quote == 'EUR':
            df['forward'] = df['spot'] - df['px'] / 1000
        df['forward'] = df['spot'] - df['px'] / 100
    else:
        df['forward'] = df['spot'] + df['px'] / 10000

    df['unhedgedCHF'] = df['Close'] * df['spot']

    # FX forward return berechnen (logarithmisch)
    df["fwd_logreturn"] = np.log(df["forward"].shift(1)) - np.log(df["spot"].shift(1))

    # FX return berechnen (logarithmisch)
    df["fx_logreturn"] = np.log(df["spot"]) - np.log(df["spot"].shift(1))

    # calculate the (negative) hedge return (hedge_cost - currency return) multiplied by the hedge ratio
    df["hedge_logreturn"] = (df["fwd_logreturn"] - df["fx_logreturn"]) * hedge_ratio

    # returns berechnen
    # df['returns'] = df['CloseUSD'].pct_change()

    # log returns berechnen
    df['local_logreturns'] = np.log(df['Close']).diff()

    # change
    df['carry'] = df['carry'].shift(1).fillna(0)
    df['total_hedged_logreturns'] = np.where(df['carry'] < 0,
                                             df['local_logreturns'] + df['fx_logreturn'] + df['hedge_logreturn'],
                                             df['local_logreturns'] + df['fx_logreturn'])

    df['unhedged_logreturns'] = df["fx_logreturn"] + df['local_logreturns']

    # CUMSUM
    df['total_hedgedcum_logreturns'] = df['total_hedged_logreturns'].cumsum()
    df['localcum_logreturns'] = df['local_logreturns'].cumsum()
    df['unhedgedcum_logreturns'] = df['unhedged_logreturns'].cumsum()

    # Rücktransformation
    df['hedged_growth'] = np.exp(df['total_hedgedcum_logreturns']).fillna(1)
    df['local_growth'] = np.exp(df['localcum_logreturns']).fillna(1)
    df['unhedged_growth'] = np.exp(df['unhedgedcum_logreturns']).fillna(1)

    if plot == True:
        # Plotten der drei Wachstumsreihen mit Labels
        df['hedged_growth'].plot(label='Hedged Growth')
        df['local_growth'].plot(label='Local Growth')
        df['unhedged_growth'].plot(label='Unhedged Growth')

        # Hinzufügen der Legende
        plt.legend()

        # Titel und Achsenbeschriftungen für bessere Übersicht
        plt.title('Wachstumsverläufe: Hedged, Local und Unhedged')
        plt.xlabel('Zeit')
        plt.ylabel('Wachstumsfaktor')

        # Zeige das Diagramm
        plt.show()

    return df


def hedge_value(CPI, fx_dataframe, asset_dataframe, quote, base='CHF', hedge_ratio=1, plot=False):
    # Erstellen einer Kopie, um Änderungen zu vermeiden
    asset_dataframe = asset_dataframe.copy()
    CPI = CPI.copy()
    fx_dataframe = fx_dataframe.copy()

    fx_dataframe['Date'] = pd.to_datetime(fx_dataframe['Date'])
    averages = []

    for index, row in fx_dataframe.iterrows():
        current_date = row['Date']

        # Calculate the start and end dates of the 4.5 to 5.5 year range
        start_date = current_date - pd.DateOffset(years=5, months=6)
        end_date = current_date - pd.DateOffset(years=4, months=6)

        # Filter rows within the range
        filtered_values = fx_dataframe.loc[
            (fx_dataframe['Date'] >= start_date) & (fx_dataframe['Date'] <= end_date), 'spot'
        ]

        # Calculate the mean, or NaN if no values exist
        averages.append(filtered_values.mean() if not filtered_values.empty else None)

    fx_dataframe["5years"] = averages

    # clean asset dataframe and fx dataframe
    asset_dataframe = asset_dataframe.filter(regex='Close|Date')  # clean asset dataframe
    fx_dataframe = fx_dataframe.filter(regex='px|Date|spot|5years')  # clean fx dataframe
    CPI = CPI.filter(regex='Date|' + quote + '|' + base)  # clean deposit rate dataframe

    CPI['Date'] = pd.to_datetime(CPI['Date'])

    fx_dataframe['Date'] = pd.to_datetime(fx_dataframe['Date'])
    asset_dataframe.reset_index(inplace=True)
    asset_dataframe = asset_dataframe.copy()
    asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date']).dt.date
    asset_dataframe['Date'] = pd.to_datetime(asset_dataframe['Date'])

    # merge asset and fx dataframes and create new dataframe df
    df = pd.merge(asset_dataframe, fx_dataframe, how='inner', on='Date')

    df = pd.merge(df, CPI, how='inner', on='Date')

    return5y = (df['spot'] - df['5years']) / df['5years']
    df['valuation_metric'] = -(return5y / df['spot']) - np.log(df[quote] / df[base])

    # calculate the forward price
    # if JPY then /100 and if EURJPY or JPYEUR then /1000 else /10000

    if base == 'JPY' or quote == 'JPY':
        if base == 'EUR' or quote == 'EUR':
            df['forward'] = df['spot'] - df['px'] / 1000
        df['forward'] = df['spot'] - df['px'] / 100
    else:
        df['forward'] = df['spot'] + df['px'] / 10000

    df['unhedgedCHF'] = df['Close'] * df['spot']

    # FX forward return berechnen (logarithmisch)
    df["fwd_logreturn"] = np.log(df["forward"].shift(1)) - np.log(df["spot"].shift(1))

    # FX return berechnen (logarithmisch)
    df["fx_logreturn"] = np.log(df["spot"]) - np.log(df["spot"].shift(1))

    # calculate the (negative) hedge return (hedge_cost - currency return) multiplied by the hedge ratio
    df["hedge_logreturn"] = (df["fwd_logreturn"] - df["fx_logreturn"]) * hedge_ratio

    # returns berechnen
    # df['returns'] = df['CloseUSD'].pct_change()

    # log returns berechnen
    df['local_logreturns'] = np.log(df['Close']).diff()

    # change
    df['valuation_metric'] = df['valuation_metric'].shift(1).fillna(0)
    df['total_hedged_logreturns'] = np.where(df['valuation_metric'] > 0,
                                             df['local_logreturns'] + df['fx_logreturn'] + df['hedge_logreturn'],
                                             df['local_logreturns'] + df['fx_logreturn'])

    df['unhedged_logreturns'] = df["fx_logreturn"] + df['local_logreturns']

    # CUMSUM
    df['total_hedgedcum_logreturns'] = df['total_hedged_logreturns'].cumsum()
    df['localcum_logreturns'] = df['local_logreturns'].cumsum()
    df['unhedgedcum_logreturns'] = df['unhedged_logreturns'].cumsum()

    # Rücktransformation
    df['hedged_growth'] = np.exp(df['total_hedgedcum_logreturns']).fillna(1)
    df['local_growth'] = np.exp(df['localcum_logreturns']).fillna(1)
    df['unhedged_growth'] = np.exp(df['unhedgedcum_logreturns']).fillna(1)

    if plot == True:
        # Plotten der drei Wachstumsreihen mit Labels
        df['hedged_growth'].plot(label='Hedged Growth')
        df['local_growth'].plot(label='Local Growth')
        df['unhedged_growth'].plot(label='Unhedged Growth')

        # Hinzufügen der Legende
        plt.legend()

        # Titel und Achsenbeschriftungen für bessere Übersicht
        plt.title('Wachstumsverläufe: Hedged, Local und Unhedged')
        plt.xlabel('Zeit')
        plt.ylabel('Wachstumsfaktor')

        # Zeige das Diagramm
        plt.show()

    return df


def get_fx(investor_currency, currency):
    """
    This function calculates the exchange rate between two currencies, either directly or via a cross-rate calculation
    if neither of the currencies is USD. The function returns a DataFrame containing the spot rate and forward points (px)
    for the given currency pair.

    Parameters:
    - investor_currency (str): The currency of the investor (e.g., 'CHF', 'EUR').
    - currency (str): The foreign currency to calculate the exchange rate for (e.g., 'JPY', 'GBP').

    Process:
    1. If one of the currencies is USD:
        - The function directly loads the exchange rate data from an Excel file, which includes the spot rate and forward points,
          and returns the data.

    2. If neither of the currencies is USD:
        - The function uses USD as an intermediary currency for the cross-rate calculation.
        - Two Excel files are loaded:
            a) Exchange rate for 'currency' to USD.
            b) Exchange rate for USD to 'investor_currency'.
        - Forward rates are calculated by combining the spot rate and forward points.
        - The cross-rate is determined by multiplying the spot and forward rates from the two exchange rates.
        - The forward points for the cross-rate are calculated and stored in a new DataFrame.
        - The 'forward' column is removed, and only the calculated values are kept.

    Return Value:
    The exchange rate for currency / investor_currency (how much the investor receives for 1 unit of the foreign currency).

    Returns:
    - pd.DataFrame: A DataFrame with the following columns:
        - 'spot': The calculated spot rate for the cross-rate.
        - 'px': The calculated forward points for the cross-rate.
        - 'Date': The date corresponding to the exchange rate data.
    """
    if currency == 'USD' or investor_currency == 'USD':  # if USD then read directly
        name = currency + investor_currency + '.csv'
        df = pd.read_csv('Daten/FX/' + name)
        return df

    # IF no USD, then we take USD as intermediary currency (Cross Rate)

    # Load the two exchange rates
    first = pd.read_csv('Daten/FX/' + currency + 'USD.csv')
    second = pd.read_csv('Daten/FX/USD' + investor_currency + '.csv')

    if currency == 'JPY':
        first['forward'] = first['spot'] + first['px'] / 100
    else:
        # Calculate the forward rate for each exchange rate
        first['forward'] = first['spot'] + first['px'] / 10000

    if investor_currency == 'JPY':
        second['forward'] = second['spot'] + second['px'] / 100
    else:
        second['forward'] = second['spot'] + second['px'] / 10000

    # Create a new DataFrame
    df = pd.DataFrame()

    # Calculate the cross rate
    df['spot'] = second['spot'] * first['spot']
    df['forward'] = second['forward'] * first['forward']

    if currency == 'JPY' or investor_currency == 'JPY':
        if currency == 'EUR' or investor_currency == 'EUR':
            df['px'] = (df['forward'] - df['spot']) * 1000

        df['px'] = (df['forward'] - df['spot']) * 100
    else:
        df['px'] = (df['forward'] - df['spot']) * 10000  # Forward points calculation

    # delete column forward
    df = df.drop(columns=['forward'])
    df['Date'] = first['Date']
    df['Date'] = pd.to_datetime(df['Date'])
    return df


def porftolio_hedge(assets, currencies, portfolio_weights, investor_currency, how='hedge', stocks=True,
                    start='2011-01-01', end='2024-09-30', num_horizon=90):
    '''

    '''
    df = pd.DataFrame()
    deposit_rate = pd.read_csv('Daten/1m_deposit_rates.csv', sep=';')
    deposit_rate['Date'] = pd.to_datetime(deposit_rate['Date'], dayfirst=True)

    cpi = pd.read_csv('Daten/cpi.csv', sep=',')
    cpi['Date'] = pd.to_datetime(cpi['Date'])

    for x in range(len(assets)):

        asset = get_asset(assets[x], start, end)
        asset = asset.groupby(asset['Date'].dt.to_period("M")).last().reset_index(drop=True)  # Monatsendkurse
        fx = get_fx(investor_currency, currencies[x])

        if stocks == False:
            asset['Close'] = 1

        if how == 'hedge':
            dat = hedge(fx, asset, currencies[x], investor_currency)

        if how == 'momentum':
            dat = hedge_momentum(fx, asset, currencies[x], num_horizon=num_horizon)

        if how == 'carry':
            dat = hedge_carry(deposit_rate, fx, asset, currencies[x], investor_currency)

        if how == 'value':
            dat = hedge_value(cpi, fx, asset, currencies[x], investor_currency)

        dat = dat.filter(regex='Date|hedged_growth|local_growth|unhedged_growth')  # nur die relevanten Spalten
        # rename
        dat.rename(columns={'hedged_growth': 'hedged_' + assets[x], 'local_growth': 'local_' + assets[x],
                            'unhedged_growth': 'unhedged' + assets[x]}, inplace=True)
        # rechne alle spalten ausser Date mal 2
        dat[dat.columns[1:]] = dat[dat.columns[1:]] * portfolio_weights[x]

        # merge
        if x == 0:
            df = dat
        else:
            df = pd.merge(df, dat, on='Date', how='left')

    df.fillna(method='ffill', inplace=True)
    df['hedged_growth'] = df.filter(regex='^hedged').sum(axis=1)

    df['local_growth'] = df.filter(regex='^local').sum(axis=1)

    df['unhedged_growth'] = df.filter(regex='^unhedged').sum(axis=1)

    # df = df.append(data)
    return df