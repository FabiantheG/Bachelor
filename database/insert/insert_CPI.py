
import pandas as pd


cpi_data =  pd.read_csv('/Users/lukas/Documents/Bachelor/Database/csv_file/cpi_ds_20101231_20250331_monthly.csv'
                        , skiprows=2, header=None , sep= ';',
                        names=["Date",
                               "JP: Japan",
                               "US: United States",
                               "SW: Switzerland",
                               "NW: Norway",
                               "SD: Sweden",
                               "CN: China",
                               "NZ: New Zealand",
                               "AU: Australia",
                               "EK: Eurozone",
                               "UK: United Kingdom"])
#print(cpi_data.head())


# ["JPY", "USD", "CHF", "NOK", "SEK", "CNY", "NZD", "AUD", "EUR", "GBP"]


# date, rate, series id
# provider_id, series_id , currency
# currency






def insert_cpi_to_db(path, provider, currency):

    cpi_data = pd.read_csv(path, skiprows=2, header=None , sep= ';',
                               names=["Date","JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"])

   long_cpi = pd.melt(
        cpi_data,
        id_vars=["Date"],
        var_name="Currency",
        value_name="CPI_Value")

    print(long_cpi.head())

print(insert_cpi_to_db( provider= 1,
                        path= '/Users/lukas/Documents/Bachelor/Database/csv_file/cpi_ds_20101231_20250331_monthly.csv'))













