from database.insert import *
from database.models import *
from database.functions import *

insert_provider = True
insert_deposit_rates = True
insert_assets = True
insert_bonds = True
insert_cpi = True
insert_fx = True
insert_gdp = True
insert_factors = True







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