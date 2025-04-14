
from database.models.provider import Provider
from database.session import session
import pandas as pd



def csv_deposit_format(currency):
    """
    Loads 1M deposit rate data for a specified currency from a local CSV file.

    The function reads a prepared CSV file containing daily 1-month deposit rates
    for various currencies, extracts the 'Date' column and the column corresponding
    to the specified currency, and returns a DataFrame with these two columns.

    Parameters:
    -----------
    currency : str
        The currency code to extract (e.g., 'USD', 'EUR', 'JPY').

    Returns:
    --------
    pd.DataFrame
        A DataFrame with two columns: 'Date' and the specified currency.
        Dates are in string format and may be converted to datetime if needed.
    """
    path = 'database/csv_file/1m_deposit_rates_ds_20101231_202503031_daily.csv'
    m1_deposit_data = pd.read_csv(path, skiprows=2, header=None, sep=';',
                           names=["Date", 'CAD', 'JPY', 'EUR', 'USD', 'AUD', 'NOK', 'SEK', 'NZD', 'CHF', 'GBP'])
    depost_rates = m1_deposit_data[['Date',currency]]
    return depost_rates


def insert_full_interest_rate(provider_name: str, currency: str, df: pd.DataFrame, duration: str):
    """
    Inserts interest rate data (e.g. 1M deposit rates) into the database:
    - Ensures the provider exists.
    - Ensures the currency/duration pair exists in INTEREST_RATE.
    - Ensures IR_REF exists for the provider/currency/duration combination.
    - Inserts only new records into IR_TS (no duplicates by date).
    - Parses date column from DD.MM.YYYY format.

    Parameters:
    -----------
    provider_name : str
        Name of the data provider (e.g. 'refinitiv').

    currency : str
        Currency code (e.g. 'USD').

    df : pd.DataFrame
        Must contain columns ['Date', currency] where 'Date' is in DD.MM.YYYY format.

    duration : str
        Duration of the interest rate (e.g. '1M' for 1-month rates).
    """
    with session:
        with session.begin():
            # Step 1: Provider
            provider = session.query(Provider).filter_by(name=provider_name).first()
            if not provider:
                provider = Provider(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Provider '{provider_name}' created with ID {provider.provider_id}")
            else:
                print(f"Provider '{provider_name}' already exists (ID {provider.provider_id})")

            # Step 2: INTEREST_RATE entry (currency + duration)
            ir = session.query(Interest_Rate).filter_by(currency=currency, duration=duration).first()
            if not ir:
                ir = Interest_Rate(currency=currency, duration=duration)
                session.add(ir)
                session.flush()
                print(f"INTEREST_RATE created for {currency}, duration '{duration}'")
            else:
                print(f"INTEREST_RATE already exists for {currency}, duration '{duration}' (ID {ir.ir_id})")

            # Step 3: IR_REF (provider + ir_id)
            ir_ref = session.query(IR_Ref).filter_by(
                provider_id=provider.provider_id,
                ir_id=ir.ir_id
            ).first()
            if not ir_ref:
                ir_ref = IR_Ref(provider_id=provider.provider_id, ir_id=ir.ir_id)
                session.add(ir_ref)
                session.flush()
                print(f"IR_REF created (series_id: {ir_ref.series_id})")
            else:
                print(f"IR_REF already exists (series_id: {ir_ref.series_id})")

            # Step 4: Validate DataFrame columns
            if "Date" not in df.columns or currency not in df.columns:
                raise ValueError(f"DataFrame must contain columns 'Date' and '{currency}'.")

            # Step 5: Convert 'Date' from string to datetime.date (assumes DD.MM.YYYY)
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date

            # Drop rows with invalid or missing data
            df = df[df["Date"].notna()]
            df = df[df[currency].notna()]

            # Step 6: Get existing dates
            existing_dates = session.query(IR_TS.date).filter_by(series_id=ir_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            # Step 7: Prepare only new entries
            new_records = []
            for _, row in df.iterrows():
                if row["Date"] not in existing_dates_set:
                    new_records.append({
                        "date": row["Date"],
                        "rate": row[currency],
                        "series_id": ir_ref.series_id
                    })

            # Step 8: Insert if needed
            if new_records:
                session.bulk_insert_mappings(IR_TS, new_records)
                print(f"Inserted {len(new_records)} new IR_TS records.")
            else:
                print("No new IR_TS records to insert – all dates already exist.")

    print("Interest Rate import completed.")


def insert_all_deposit_rates(provider_name: str, duration: str = "1M"):
    """
    Loads and inserts 1M deposit rate data for a predefined list of currencies into the database.

    For each currency:
    - Loads data from the CSV file using `csv_deposit_format`.
    - Inserts provider, currency, IR_REF and IR_TS entries (skipping existing).
    - Prints progress and errors per currency.

    Parameters:
    -----------
    provider_name : str
        Name of the data provider (e.g. 'bloomberg').

    duration : str
        Duration of the interest rate (default is '1M').
    """
    currencies = ["JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"]

    for currency in currencies:
        try:
            print(f"\nProcessing currency: {currency}")
            df = csv_deposit_format(currency)
            insert_full_interest_rate(provider_name=provider_name, currency=currency, df=df, duration=duration)
        except Exception as e:
            print(f"Error processing {currency}: {e}")

# Aufruf der Funktion
#insert_all_deposit_rates(provider_name="bloomberg", duration="1M")



