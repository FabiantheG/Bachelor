from database.models import *
from database.session import session
import pandas as pd

def insert_factor(name, df, version=1):
    """
    Insert a new factor and its associated timeseries data into the database.

    :param name: Name of the factor.
    :param df: Pandas DataFrame with 'date' as index and one data column (rate).
    :param version: Version number of the factor.
    """
    try:
        # Step 1: Check if factor exists
        factor = session.query(FACTOR).filter_by(name_factor=name, version=version).first()
        if not factor:
            factor = FACTOR(name_factor=name, version=version)
            session.add(factor)
            session.flush()  # makes factor_id available immediately
            print(f"Factor '{name}' created (ID: {factor.factor_id}).")
        else:
            print(f"Factor '{name}' already exists (ID: {factor.factor_id}).")

        # Step 2: Check if factor_ref exists
        fac_ref = session.query(FACTOR_REF).filter_by(factor_id=factor.factor_id).first()
        if not fac_ref:
            fac_ref = FACTOR_REF(factor_id=factor.factor_id)
            session.add(fac_ref)
            session.flush()
            print(f"FACTOR_REF created (series_id: {fac_ref.series_id}).")
        else:
            print(f"FACTOR_REF already exists (series_id: {fac_ref.series_id}).")

        # Step 3: Prepare dataframe
        df = df.reset_index()
        if df.columns[1] != 'rate':
            df.columns.values[1] = 'rate'

        # Step 4: Filter only new dates
        existing_dates = session.query(FACTOR_TS.date).filter_by(series_id=fac_ref.series_id).all()
        existing_dates_set = {d[0] for d in existing_dates}

        new_records = [
            {"date": row["date"], "rate": row["rate"], "series_id": fac_ref.series_id}
            for _, row in df.iterrows()
            if row["date"] not in existing_dates_set
        ]

        # Step 5: Insert new records
        if new_records:
            session.bulk_insert_mappings(FACTOR_TS, new_records)
            print(f"Inserted {len(new_records)} new FACTOR_TS records.")
        else:
            print("No new FACTOR_TS records to insert – all dates already exist.")

        # Step 6: Commit changes
        session.commit()

    except Exception as e:
        session.rollback()
        print(f"Error inserting factor '{name}':", e)


