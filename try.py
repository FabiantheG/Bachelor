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



#insert_portfolio(portfolio_name, investor_cur, asset_ticker, weight)


#print(get_portfolio('World_Equal'))






sim_id = insert_simulation(
    portfolio_name='World_Equal',
    strategy_name='ABC_Hedge',
    unhedged_growth=[0.98, 0.981,0.98, 0.981,0.98, 0.981,0.98, 0.981,0.98, 0.981],
    hedged_growth=[0.93, 0.938,0.98, 0.981,0.98, 0.981,0.98, 0.981,0.98, 0.981])







