
from database.functions import *
from database.insert import *


g10 =  ['CHF','EUR', 'JPY',  'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK','USD']
current = 0
for base in g10:
    current += 1
    dollar = create_dollar_factor(base)
    carry = create_carry_factor(base)

    insert_factor('dollar'+base,dollar)
    insert_factor('carry'+base,carry)
    print(str(current) + '/' + str(len(g10)))





