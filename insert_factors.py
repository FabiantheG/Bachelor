
from database.functions import *
from database.insert import *


dolcar = True

if dolcar:
    g10 =  ['CHF','EUR', 'JPY',  'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK','USD']
    current = 0
    for base in g10:
        current += 1
        dollar = create_dollar_factor(base)
        carry = create_carry_factor(base)

        insert_factor('dollar'+base,dollar)
        insert_factor('carry'+base,carry)
        print(str(current) + '/' + str(len(g10)))

for base in ['EUR', 'JPY',  'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK','USD','CHF']:
    volatility = create_volatility_factor(base)
    afd = create_afd_factor(base)
    insert_factor('volatility'+base,volatility)
    insert_factor('afd'+base,afd)


commodity = create_commodity_factor()
ted = create_ted_factor()

insert_factor('ted',ted)
insert_factor('commodity',commodity)



