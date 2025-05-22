from simulation.functions import *
from database.functions import *

#get_fx('USD','CHF','Spot')
#df = create_volatility_factor('CHF')
#df2 = create_afd_factor('CHF')


x = get_factor('afdCHF')
x2 = get_factor('volatilityCHF')
x3 = get_factor('ted')
x4 = get_factor('commodity')


print(x)
print(x2)
print(x3)
print(x4)









