from database.models import *
from database.models import INTEREST_RATE
from database.session import session
import pandas as pd


def csv_deposit_format(currency):
    """
    Load 1-month deposit rates from a CSV file.

    Reads a preformatted CSV file containing daily 1-month deposit rates for various currencies,
    extracts the 'Date' column and the column corresponding to the given currency, and returns them.

    :param currency: The currency code to extract (e.g., 'USD', 'EUR', 'JPY').
    :type currency: str
    :return: DataFrame with two columns ['Date', currency].
    :rtype: pandas.DataFrame
    :raises FileNotFoundError: If the CSV file is not found.
    :raises KeyError: If the specified currency is not present in the data.
    """
    path = 'database/csv_file/1m_deposit_rates_ds_20101231_202503031_daily.csv'
    m1_deposit_data = pd.read_csv(path, skiprows=2, header=None, sep=';',
                                  names=["Date", 'CAD', 'JPY', 'EUR', 'USD',
                                         'AUD', 'NOK', 'SEK', 'NZD', 'CHF', 'GBP'])
    if currency not in m1_deposit_data.columns:
        raise KeyError(f"Currency '{currency}' not found in CSV columns")
    deposit_rates = m1_deposit_data[['Date', currency]]
    return deposit_rates


def insert_full_interest_rate(provider_name: str, currency: str, df: pd.DataFrame, duration: str):
    """
    Insert 1-month interest rate data into the database.

    Ensures the provider exists, the INTEREST_RATE entry (currency+duration) exists,
    and the IR_REF linking provider and rate exists. Parses dates, filters out duplicates,
    and bulk-inserts new rows into IR_TS.

    :param provider_name: Name of the data provider (e.g. 'refinitiv').
    :type provider_name: str
    :param currency: Currency code (e.g. 'USD').
    :type currency: str
    :param df: DataFrame with columns ['Date', currency], where 'Date' is 'DD.MM.YYYY'.
    :type df: pandas.DataFrame
    :param duration: Duration identifier (e.g. '1M').
    :type duration: str
    :raises ValueError: If required columns are missing in `df`.
    """
    with session:
        with session.begin():
            # Step 1: Provider
            provider = session.query(PROVIDER).filter_by(name=provider_name).first()
            if not provider:
                provider = PROVIDER(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Provider '{provider_name}' created with ID {provider.provider_id}")
            else:
                print(f"Provider '{provider_name}' already exists (ID {provider.provider_id})")

            # Step 2: INTEREST_RATE
            ir = session.query(INTEREST_RATE).filter_by(currency=currency, duration=duration).first()
            if not ir:
                ir = INTEREST_RATE(currency=currency, duration=duration)
                session.add(ir)
                session.flush()
                print(f"INTEREST_RATE created for {currency}, duration '{duration}'")
            else:
                print(f"INTEREST_RATE already exists for {currency}, duration '{duration}' (ID {ir.ir_id})")

            # Step 3: IR_REF
            ir_ref = session.query(IR_REF).filter_by(provider_id=provider.provider_id, ir_id=ir.ir_id).first()
            if not ir_ref:
                ir_ref = IR_REF(provider_id=provider.provider_id, ir_id=ir.ir_id)
                session.add(ir_ref)
                session.flush()
                print(f"IR_REF created (series_id: {ir_ref.series_id})")
            else:
                print(f"IR_REF already exists (series_id: {ir_ref.series_id})")

            # Step 4: Validate DataFrame
            if "Date" not in df.columns or currency not in df.columns:
                raise ValueError(f"DataFrame must contain 'Date' and '{currency}' columns")

            # Step 5: Parse and filter
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date
            df = df[df["Date"].notna() & df[currency].notna()]

            # Step 6: Exclude existing dates
            existing_dates = {d[0] for d in session.query(IR_TS.date).filter_by(series_id=ir_ref.series_id).all()}

            # Step 7: Prepare new records
            new_records = [
                {"date": row["Date"], "rate": row[currency], "series_id": ir_ref.series_id}
                for _, row in df.iterrows() if row["Date"] not in existing_dates
            ]

            # Step 8: Bulk insert
            if new_records:
                session.bulk_insert_mappings(IR_TS, new_records)
                print(f"Inserted {len(new_records)} new IR_TS records.")
            else:
                print("No new IR_TS records to insert – all dates already exist")

    print("Interest rate import completed.")


def insert_all_deposit_rates(provider_name: str, duration: str = "1M"):
    """
    Load and insert 1-month deposit rates for multiple currencies.

    Iterates through a predefined currency list, calls `csv_deposit_format`,
    then `insert_full_interest_rate`, skipping duplicates and logging progress.

    :param provider_name: Name of the data provider (e.g. 'bloomberg').
    :type provider_name: str
    :param duration: Duration identifier (default '1M').
    :type duration: str
    """
    currencies = ["JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"]

    for currency in currencies:
        try:
            print(f"\nProcessing currency: {currency}")
            df = csv_deposit_format(currency)
            insert_full_interest_rate(provider_name=provider_name, currency=currency, df=df, duration=duration)
        except Exception as e:
            print(f"Error processing {currency}: {e}")
