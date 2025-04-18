from database.insert import *
from database.models import *



# Provider
insert_new_provider('bloomberg2323222')

# Deposit rates
insert_all_deposit_rates(provider_name="bloomberg", duration="1M")

# Assets
insert_all_msci_assets(provider_name="bloomberg")

#Bonds
insert_all_msci_bonds(provider_name="bloomberg")

# CPI
insert_all_cpi_currencies(provider_name='bloomberg')

# FX
insert_all_fx_forward()
insert_all_spot_data()

# GDP
insert_all_gdp()



