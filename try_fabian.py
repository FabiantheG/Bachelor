from database.insert import *
from database.models import *
from database.functions import *
from database.session import session
from simulation.functions import *
import pandas as pd

x = get_portfolio_data(['MSCI_UK','MSCI_USA'])

df = simulate_portfolio(x[0],x[1],x[2],x[3],weights = [0.5,0.5])



#metrics = metric_simulation(df)


#plot_simulation(df,save=True)

#insert_portfolio('portfolio1','CHF',['MSCI_UK','MSCI_USA'],[0.5,0.5])

insert_simulation('portfolio1','name',1,df)
new = get_simulation('portfolio1', 'name', 1)

metrics = metric_simulation(new)
print(metrics)