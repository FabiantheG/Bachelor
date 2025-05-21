from simulation.functions import *
from database.functions import *

#get_fx('USD','CHF','Spot')
df = create_volatility_factor('CHF')
df2 = create_afd_factor('CHF')
print(df.columns)

print(df)


print(df2)









