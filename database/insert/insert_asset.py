from database.models import *
from database.models import ASSET
from database.session import session
import pandas as pd


def csv_msci_format(index_name: str) -> pd.DataFrame:
    """
    Load MSCI equity index data for a specified index from CSV.

    Reads a local CSV file containing daily MSCI equity index values,
    extracts the Date column and the column matching the given index_name,
    and returns a DataFrame with those two columns.

    :param index_name: The MSCI index name to extract (e.g., 'MSCI USA', 'MSCI Europe').
    :type index_name: str
    :raises ValueError: If the specified index_name is not found in the CSV file.
    :return: DataFrame with columns ['Date', index_name].
    :rtype: pandas.DataFrame
    """
    path = 'database/csv_file/msci_eq_idx_19991231_20250331_daily.csv.csv'
    msci_eq_index = pd.read_csv(
        path,
        sep=',',
        skiprows=1,
        header=None,
        names=[
            "Date", "MSCI Australia", "MSCI Canada", "MSCI Europe", "MSCI Japan",
            "MSCI New Zealand", "MSCI Norway", "MSCI Sweden", "MSCI Switzerland",
            "MSCI UK", "MSCI USA"
        ]
    )

    if index_name not in msci_eq_index.columns:
        raise ValueError(f"Index '{index_name}' not found in MSCI file.")

    msci_data = msci_eq_index[['Date', index_name]]
    return msci_data


def insert_full_asset(provider_name: str, asset_ticker: str, currency: str, df: pd.DataFrame) -> None:
    """
    Insert MSCI equity index time series into the database.

    Ensures that the provider, asset, and asset_ref records exist. Renames the
    DataFrame's series column to asset_ticker, parses 'Date' to datetime.date,
    filters out missing values, and bulk-inserts only new records into ASSET_TS.

    :param provider_name: Name of the data provider (e.g., 'bloomberg').
    :type provider_name: str
    :param asset_ticker: Ticker to assign to this index (e.g., 'MSCI_USA').
    :type asset_ticker: str
    :param currency: ISO currency code of the index values (e.g., 'USD').
    :type currency: str
    :param df: DataFrame with columns ['Date', <original index column>].
    :type df: pandas.DataFrame
    :raises ValueError: If the DataFrame does not contain exactly 'Date' and one data column.
    :return: None
    :rtype: None
    """
    with session:
        with session.begin():
            # 1. Provider
            provider = session.query(PROVIDER).filter_by(name=provider_name).first()
            if not provider:
                provider = PROVIDER(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Created new provider '{provider_name}' (ID {provider.provider_id})")
            else:
                print(f"Using existing provider '{provider_name}' (ID {provider.provider_id})")

            # 2. Asset
            asset = session.query(ASSET).filter_by(asset_ticker=asset_ticker).first()
            if not asset:
                asset = ASSET(asset_ticker=asset_ticker, currency=currency)
                session.add(asset)
                session.flush()
                print(f"Inserted new asset '{asset_ticker}'")
            else:
                print(f"Asset '{asset_ticker}' already exists")

            # 3. Asset_Ref
            asset_ref = session.query(ASSET_REF).filter_by(
                provider_id=provider.provider_id,
                asset_ticker=asset_ticker
            ).first()
            if not asset_ref:
                asset_ref = ASSET_REF(provider_id=provider.provider_id, asset_ticker=asset_ticker)
                session.add(asset_ref)
                session.flush()
                print(f"Created asset_ref (series_id: {asset_ref.series_id})")
            else:
                print(f"Asset_ref already exists (series_id: {asset_ref.series_id})")

            # 4. Rename DataFrame column to asset_ticker
            value_column = [col for col in df.columns if col != "Date"][0]
            if value_column != asset_ticker:
                df = df.rename(columns={value_column: asset_ticker})

            # 5. Clean DataFrame
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
            df = df[df["Date"].notna() & df[asset_ticker].notna()]

            # 6. Get existing dates
            existing_dates = session.query(ASSET_TS.date).filter_by(series_id=asset_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            # 7. Prepare new rows
            new_rows = [
                {"date": row["Date"], "close": row[asset_ticker], "series_id": asset_ref.series_id}
                for _, row in df.iterrows()
                if row["Date"] not in existing_dates_set
            ]

            # 8. Bulk insert
            if new_rows:
                session.bulk_insert_mappings(ASSET_TS, new_rows)
                print(f"Inserted {len(new_rows)} new records into ASSET_TS.")
            else:
                print("No new asset time series data to insert.")

    print("Asset import completed.")


def insert_all_msci_assets(provider_name: str) -> None:
    """
    Load and insert all MSCI equity indices into the database.

    Iterates over a mapping of MSCI index names to currencies, loads each series
    via `csv_msci_format`, and calls `insert_full_asset` for insertion.
    Skips any series that are already fully loaded.

    :param provider_name: Name of the data provider (e.g., 'bloomberg').
    :type provider_name: str
    :return: None
    :rtype: None
    """
    index_currency_map = {
        "MSCI Australia": "AUD",
        "MSCI Canada": "CAD",
        "MSCI Europe": "EUR",
        "MSCI Japan": "JPY",
        "MSCI New Zealand": "NZD",
        "MSCI Norway": "NOK",
        "MSCI Sweden": "SEK",
        "MSCI Switzerland": "CHF",
        "MSCI UK": "GBP",
        "MSCI USA": "USD"
    }

    for index_name, currency in index_currency_map.items():
        asset_ticker = index_name.replace(" ", "_")

        try:
            print(f"\nProcessing: {index_name} ({currency})")
            df = csv_msci_format(index_name)
            insert_full_asset(
                provider_name=provider_name,
                asset_ticker=asset_ticker,
                currency=currency,
                df=df
            )
        except Exception as e:
            print(f"Error while processing {index_name}: {e}")
