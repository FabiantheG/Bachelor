from database.functions import get_factor
import pandas as pd



def get_factors_simulation(base, cur_list, list_factors):

    list = []



    if 'dollar' in list_factors:
        dollar = get_factor('dollar'+base)
        dollar.columns = ['dollar']
        list.append(dollar)

    if 'carry' in list_factors:
        carry = get_factor('carry'+base)
        carry.columns = ['carry']
        list.append(carry)

    for cur in cur_list:
        if 'volatility' in list_factors:
            vol = get_factor('volatility' + cur + base)
            vol.columns = ['volatility'+cur + base]
            list.append(vol)



    factors = pd.concat(list,axis = 1,join='inner')

    return factors