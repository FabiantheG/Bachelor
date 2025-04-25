from database.insert import *
from database.models import *
from database.functions import *




'''
x = get_fx('USD','AUD','1M')
x2 = get_fx('USD','AUD','Spot')

x_new = pd.merge(x, x2, left_index=True, right_index=True)
print(x_new)
#print(get_fx('USD','AUD','Spot'))
#print(get_ir('USD'))
'''





portfolio_name = 'World_Equal'
asset_ticker = ['MSCI_Australia', 'MSCI_Canada', 'MSCI_Europe', 'MSCI_Japan',
'MSCI_New_Zealand', 'MSCI_Norway', 'MSCI_Sweden','MSCI_Switzerland','MSCI_UK', 'MSCI_USA']
weight = [0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]
investor_cur = 'CHF'

insert_portfolio(portfolio_name, investor_cur, asset_ticker, weight)





#print(get_fx('USD','AUD','Spot'))


#print(get_portfolio('World_Equal')) # gives an error because of session.begin(): prevent, execute last line







# Example-function to insert simulation data
data = {"date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
    "hedged_growth": [5, 1.03, 1.01, 1.04], "unhedged_growth": [9, 1.05, 1.00, 1.03]}
df = pd.DataFrame(data)
# Set the date column as the index
df.set_index("date", inplace=True)
# Example parameters for the function
portfolio_name = "World_Equal"
hedge_name = "DCF_Hedge"
version = 1.1
# Test the function
insert_simulation(portfolio_name, hedge_name, version, df)


# Test get simulation function
print(get_simulation(portfolio_name="World_Equal", hedge_name="DCF_Hedge", version=1.1))







