


from database.models import *
from database.session import session
from database.functions import *
import pandas as pd
import numpy as np



def create_fxvolatility_factor(base,quote):
    df = get_fx(quote, base, duration='Spot')

    # Berechne Rolling-Volatilität direkt
    df_new = pd.DataFrame(index=df.index)
    df_new['rate'] = df['rate'].rolling(30).std()

    # Entferne NaNs
    df_new = df_new.dropna()

    df_new['rate'] = df_new['rate'].astype(float)

    return  df_new



