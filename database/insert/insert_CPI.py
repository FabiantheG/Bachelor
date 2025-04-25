from database.models import *
from database.models import CPI_RATES
from database.session import session
import pandas as pd


def csv_cpi_format(currency):
    """
    Load CPI time series for a given currency from CSV.

    Reads a predefined CPI dataset of monthly values for multiple currencies,
    extracts the Date column and the specified currency column.

    :param currency: The ISO currency code to extract (e.g., 'USD', 'EUR', 'JPY').
    :type currency: str
    :return: DataFrame with columns ['Date', currency]; Date strings may need conversion.
    :rtype: pandas.DataFrame
    :raises FileNotFoundError: If the CSV file cannot be found at the expected path.
    :raises KeyError: If the specified currency is not present in the CSV columns.
    """
    path = 'database/csv_file/cpi_ds_20101231_20250331_monthly.csv'
    cpi_data = pd.read_csv(
        path,
        skiprows=2,
        header=None,
        sep=';',
        names=["Date", "JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"]
    )
    if currency not in cpi_data.columns:
        raise KeyError(f"Currency '{currency}' not found in CSV columns: {cpi_data.columns.tolist()}")
    return cpi_data[['Date', currency]]


def insert_full_cpi(provider_name: str, currency: str, df: pd.DataFrame):
    """
    Insert CPI data into the database and register it as an economic indicator.

    Ensures:
    - The provider exists (or is created).
    - The currency is present in CPI_RATES.
    - A CPI_REF and associated ECONOMIC_INDICATOR entry exist.
    - Only new date/rate pairs are bulk‐inserted into CPI_TS.

    :param provider_name: Name of the data provider (e.g. 'bloomberg').
    :type provider_name: str
    :param currency: Currency code of the CPI series (e.g. 'USD').
    :type currency: str
    :param df: DataFrame with columns ['Date', currency], where 'Date' may be a string.
    :type df: pandas.DataFrame
    :raises ValueError: If required columns 'Date' or the currency column are missing in `df`.
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

            # Step 2: CPI_RATES
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

            # Step 4: Validate DataFrame
            if "Date" not in df.columns or currency not in df.columns:
                raise ValueError(f"DataFrame must contain columns 'Date' and '{currency}'.")

            # Step 5: Parse and clean
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date
            df = df[df["Date"].notna() & df[currency].notna()]

            # Step 6: Identify new dates
            existing_dates = {d[0] for d in session.query(CPI_TS.date).filter_by(series_id=cpi_ref.series_id).all()}

            # Step 7: Prepare and insert
            new_records = [
                {"date": row["Date"], "rate": row[currency], "series_id": cpi_ref.series_id}
                for _, row in df.iterrows() if row["Date"] not in existing_dates
            ]
            if new_records:
                session.bulk_insert_mappings(CPI_TS, new_records)
                print(f"Inserted {len(new_records)} new CPI_TS records.")
            else:
                print("No new CPI_TS records to insert – all dates already exist.")

    print(f"Finished CPI import for {currency}.\n")


def insert_all_cpi_currencies(provider_name: str):
    """
    Load and insert CPI data for a fixed list of currencies.

    Iterates through G10 currencies, calls `csv_cpi_format`, then `insert_full_cpi`,
    skipping already‐loaded dates, and logs progress or errors.

    :param provider_name: Name of the data provider (e.g. 'bloomberg').
    :type provider_name: str
    """
    currencies = ["JPY", "USD", "CHF", "NOK", "SEK", "CAD", "NZD", "AUD", "EUR", "GBP"]

    for currency in currencies:
        try:
            print(f"\nProcessing CPI for: {currency}")
            cpi_df = csv_cpi_format(currency)
            insert_full_cpi(provider_name=provider_name, currency=currency, df=cpi_df)
        except Exception as e:
            print(f"Error processing {currency}: {e}")
