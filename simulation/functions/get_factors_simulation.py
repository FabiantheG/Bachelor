from database.functions import get_factor
import pandas as pd



def get_factors_simulation(base):

    list = []




    dollar = get_factor('dollar'+base)
    dollar.columns = ['dollar']
    list.append(dollar)


    carry = get_factor('carry'+base)
    carry.columns = ['carry']
    list.append(carry)

    volatility = get_factor('volatility'+base)
    volatility.columns = ['volatility']
    list.append(volatility)

    ted = get_factor('ted')
    ted.columns = ['ted']
    list.append(ted)

    commodity = get_factor('commodity')
    commodity.columns = ['commodity']
    list.append(commodity)

    afd = get_factor('afd'+base)
    afd.columns = ['afd']
    list.append(afd)


    factors = pd.concat(list,axis = 1,join='inner')

    return factors