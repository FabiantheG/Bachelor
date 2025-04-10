
from models.provider import Provider
from models.cpi import CPI_Rates, CPI_Ref, CPI_TS
from session import session
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
    path = '/Users/lukas/Documents/Bachelor/database/csv_file/cpi_ds_20101231_20250331_monthly.csv'
    cpi_data = pd.read_csv(path, skiprows=2, header=None, sep=';',
                           names=["Date", "JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"])
    cpi = cpi_data[['Date',currency]]
    return cpi


def insert_full_cpi(provider_name: str, currency: str, df: pd.DataFrame):
    """
    Inserts CPI data into the database:
    - Ensures the provider exists.
    - Ensures the currency is registered.
    - Ensures CPI_REF exists for the provider/currency combination.
    - Inserts only new records into CPI_TS (no duplicates by date).
    - Parses date column from DD.MM.YYYY format.

    Parameters:
    -----------
    provider_name : str
        Name of the data provider (e.g. 'bloomberg').

    currency : str
        Currency column in the DataFrame (e.g. 'USD').

    df : pd.DataFrame
        Must contain columns ['Date', currency] where Date is a string in format DD.MM.YYYY.
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

            # Step 2: Currency in CPI_RATES
            if not session.query(CPI_Rates).filter_by(currency=currency).first():
                session.add(CPI_Rates(currency=currency))
                print(f"Currency '{currency}' inserted into CPI_RATES.")
            else:
                print(f"Currency '{currency}' already exists in CPI_RATES.")

            # Step 3: CPI_REF
            cpi_ref = session.query(CPI_Ref).filter_by(
                provider_id=provider.provider_id,
                currency=currency
            ).first()
            if not cpi_ref:
                cpi_ref = CPI_Ref(provider_id=provider.provider_id, currency=currency)
                session.add(cpi_ref)
                session.flush()
                print(f"CPI_REF created (series_id: {cpi_ref.series_id})")
            else:
                print(f"CPI_REF already exists (series_id: {cpi_ref.series_id})")

            # Step 4: Validate DataFrame columns
            if "Date" not in df.columns or currency not in df.columns:
                raise ValueError(f"DataFrame must contain columns 'Date' and '{currency}'.")

            # Step 5: Convert 'Date' from string to datetime.date (assumes DD.MM.YYYY)
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date

            # Drop rows with invalid or missing date or rate
            df = df[df["Date"].notna()]
            df = df[df[currency].notna()]

            # Step 6: Get existing dates from DB
            existing_dates = session.query(CPI_TS.date).filter_by(series_id=cpi_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            # Step 7: Prepare only new entries
            new_records = []
            for _, row in df.iterrows():
                if row["Date"] not in existing_dates_set:
                    new_records.append({
                        "date": row["Date"],
                        "rate": row[currency],
                        "series_id": cpi_ref.series_id
                    })

            # Step 8: Insert if needed
            if new_records:
                session.bulk_insert_mappings(CPI_TS, new_records)
                print(f"Inserted {len(new_records)} new CPI_TS records.")
            else:
                print("No new CPI_TS records to insert – all dates already exist.")

    print("CPI import completed.")


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

insert_all_cpi_currencies(provider_name='bloomberg')










