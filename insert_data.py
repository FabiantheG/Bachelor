from database.insert import *
from database.models import *
from database.functions import *

insert_provider = False
insert_deposit_rates = False
insert_assets = False
insert_bonds = False
insert_cpi = False
insert_fx = False
insert_gdp = False
insert_factors = False
insert_portfolios = True






if insert_provider:
    insert_new_provider('bloomberg')

if insert_deposit_rates:
    insert_all_deposit_rates(provider_name="bloomberg", duration="1M")


if insert_assets:
    insert_all_msci_assets(provider_name="bloomberg")


if insert_bonds:
    insert_all_msci_bonds(provider_name="bloomberg")


if insert_cpi:
    insert_all_cpi_currencies(provider_name="bloomberg")


if insert_fx:
    insert_all_spot_data()
    insert_all_fx_forward()




if insert_gdp:
    insert_all_gdp()


if insert_factor:
    dollar = create_dollar_factor()
    carry = create_carry_factor()

    insert_factor('dollar',dollar)
    insert_factor('carry',carry)


if insert_portfolios:
    g10 = ['AUD', 'CAD', 'EUR', 'JPY', 'NZD', 'NOK', 'SEK', 'CHF', 'GBP', 'USD']
    tickers = [
        "MSCI_Australia", "MSCI_Canada", "MSCI_Europe", "MSCI_Japan",
        "MSCI_New_Zealand", "MSCI_Norway", "MSCI_Sweden", "MSCI_Switzerland",
        "MSCI_UK", "MSCI_USA"
    ]
    for cur in g10:
        name = 'portfolio' + cur
        namefx = 'portfolioFX' + cur
        index = g10.index(cur)

        tickers_subset = tickers[:index] + tickers[index + 1:]
        weights = [1/ len(tickers_subset) for x in tickers_subset]

        insert_portfolio(namefx, cur, tickers_subset, weights)
        insert_portfolio(name, cur, tickers_subset, weights)