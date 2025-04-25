from database.insert import *
from database.models import *
from database.functions import *
from database.session import *




'''
x = get_fx('USD','AUD','1M')
x2 = get_fx('USD','AUD','Spot')

x_new = pd.merge(x, x2, left_index=True, right_index=True)
print(x_new)
#print(get_fx('USD','AUD','Spot'))
#print(get_ir('USD'))
'''




#insert_portfolio(portfolio_name, investor_cur, asset_ticker, weight)


#print(get_portfolio('World_Equal'))



df = create_dollar_factor()


insert_factor('fdsdfs',df,1)




