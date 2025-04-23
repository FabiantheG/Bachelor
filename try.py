from database.insert import *
from database.models import *
from database.functions import *





x = get_fx('USD','AUD','1M')
x2 = get_fx('USD','AUD','Spot')

x_new = pd.merge(x, x2, left_index=True, right_index=True)
print(x_new)
#print(get_fx('USD','AUD','Spot'))
#print(get_ir('USD'))
