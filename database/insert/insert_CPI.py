from database.models import *
from database.models import CPI_RATES
from database.session import session
import pandas as pd

def csv_cpi_format(currency):
    """
    Loads CPI time series data for a specified currency from a local CSV file.

    The function reads a predefined CPI dataset containing monthly values for
    multiple currencies, extracts the 'Date' column and the column corresponding
    to the specified currency, and returns a DataFrame with those two columns.

    Parameters:
    -----------
    currency : str
        The currency code to extract (e.g., 'USD', 'EUR', 'JPY').

    Returns:
    --------
    pd.DataFrame
        A DataFrame with two columns: 'Date' and the specified currency.
        Dates are still in string format and may require conversion to datetime.
    """
    path = 'database/csv_file/cpi_ds_20101231_20250331_monthly.csv'
    cpi_data = pd.read_csv(path, skiprows=2, header=None, sep=';',
                           names=["Date", "JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"])
    cpi = cpi_data[['Date',currency]]
    return cpi

def insert_full_cpi(provider_name: str, currency: str, df: pd.DataFrame):
    """
    Inserts CPI data into the database and creates an entry in the Economic_Indicator table.
    - Ensures provider and currency are registered
    - Creates CPI_REF and Economic_Indicator entries if not present
    - Inserts only new time series values into CPI_TS (based on date)
    """
    with session:
        with session.begin():
            # Step 1: Provider
            provider = session.query(PROVIDER).filter_by(name=provider_name).first()
            if not provider:
                provider = PROVIDER(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Provider '{provider_name}' created (ID: {provider.provider_id})")
            else:
                print(f"Provider '{provider_name}' already exists (ID: {provider.provider_id})")

            # Step 2: Currency in CPI_RATES
            if not session.query(CPI_RATES).filter_by(currency=currency).first():
                session.add(CPI_RATES(currency=currency))
                print(f"Currency '{currency}' added to CPI_RATES.")
            else:
                print(f"Currency '{currency}' already exists in CPI_RATES.")

            # Step 3: CPI_REF + Economic_Indicator
            cpi_ref = session.query(CPI_REF).filter_by(
                provider_id=provider.provider_id,
                currency=currency
            ).first()

            if not cpi_ref:
                cpi_ref = CPI_REF(provider_id=provider.provider_id, currency=currency)
                session.add(cpi_ref)
                session.flush()
                print(f"CPI_REF created (series_id: {cpi_ref.series_id})")

                ei = ECONOMIC_INDICATOR(series_id=cpi_ref.series_id, indicator_type='CPI')
                session.add(ei)
                print(f"Economic_Indicator entry created for CPI_REF (series_id: {cpi_ref.series_id})")
            else:
                print(f"CPI_REF already exists (series_id: {cpi_ref.series_id})")

            # Step 4: Validate DataFrame columns
            if "Date" not in df.columns or currency not in df.columns:
                raise ValueError(f"DataFrame must contain columns 'Date' and '{currency}'.")

            # Step 5: Format and clean date and rate values
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date
            df = df[df["Date"].notna() & df[currency].notna()]

            # Step 6: Filter only new dates
            existing_dates = session.query(CPI_TS.date).filter_by(series_id=cpi_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            new_records = [
                {"date": row["Date"], "rate": row[currency], "series_id": cpi_ref.series_id}
                for _, row in df.iterrows() if row["Date"] not in existing_dates_set
            ]

            # Step 7: Insert new records
            if new_records:
                session.bulk_insert_mappings(CPI_TS, new_records)
                print(f"Inserted {len(new_records)} new CPI_TS records.")
            else:
                print("No new CPI_TS records to insert – all dates already exist.")

    print(f"Finished CPI import for {currency}.\n")

def insert_all_cpi_currencies(provider_name: str):
    """
    Loads and inserts CPI data for a predefined list of currencies.
    """
    currencies = ["JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"]

    for currency in currencies:
        try:
            print(f"Processing CPI for: {currency}")
            cpi_df = csv_cpi_format(currency)
            insert_full_cpi(provider_name=provider_name, currency=currency, df=cpi_df)
        except Exception as e:
            print(f"Error processing {currency}: {e}")

def insert_all_cpi_currencies(provider_name: str):
    """
    Loads and inserts CPI data for a predefined list of currencies into the database.

    For each currency:
    - Loads data from the CSV file using `csv_cpi_format`.
    - Inserts provider, currency, CPI_REF and CPI_TS entries (skipping existing).
    - Prints progress and errors per currency.

    Parameters:
    -----------
    provider_name : str
        Name of the data provider (e.g. 'bloomberg').
    """
    currencies = ["JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"]

    for currency in currencies:
        try:
            print(f"\nProcessing currency: {currency}")
            cpi_df = csv_cpi_format(currency)
            insert_full_cpi(provider_name=provider_name, currency=currency, df=cpi_df)
        except Exception as e:
            print(f"Error processing {currency}: {e}")












