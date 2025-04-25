import pandas as pd

def orm_list_to_df(orm_result, date_index=True):
    """
    Convert a list of SQLAlchemy ORM objects into a pandas DataFrame.

    This function builds a DataFrame from the __dict__ of each ORM instance in the provided list,
    drops the internal SQLAlchemy state column, and—if requested—parses the 'date' column as datetime
    and sets it as the DataFrame index.

    :param orm_result: List of SQLAlchemy ORM instances to convert.
    :type orm_result: list
    :param date_index: Whether to convert the 'date' column to datetime and set it as the index.
                       Defaults to True.
    :type date_index: bool, optional
    :return: DataFrame containing the ORM data, with 'date' as index if date_index is True.
    :rtype: pandas.DataFrame
    """
    df = pd.DataFrame([row.__dict__ for row in orm_result])
    df = df.drop(columns=["_sa_instance_state"], errors="ignore")

    if date_index:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

    return df
