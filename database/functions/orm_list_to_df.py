import pandas as pd

# function for turning ORM to df
def orm_list_to_df(orm_result,date_index = True):
    df = pd.DataFrame([row.__dict__ for row in orm_result])
    df = df.drop(columns=["_sa_instance_state"], errors="ignore")

    if date_index == True:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)  # set date as index
    return df