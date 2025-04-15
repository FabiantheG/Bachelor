
from database.models import *
from database.models import Asset
from database.session import session
import pandas as pd


def csv_msci_format(index_name: str):
    """
    Loads MSCI equity index data for a specified index name from a local CSV file.

    The function reads a prepared CSV file containing daily MSCI equity index values
    for various regions, extracts the 'Date' column and the column corresponding
    to the specified index, and returns a DataFrame with these two columns.

    Parameters:
    -----------
    index_name : str
        The MSCI index name to extract (e.g., 'MSCI USA', 'MSCI Europe').

    Returns:
    --------
    pd.DataFrame
        A DataFrame with two columns: 'Date' and the specified index.
        Dates are in string format and may be converted to datetime if needed.
    """
    path = 'database/csv_file/msci_eq_idx_19991231_20250331_daily.csv.csv'
    msci_eq_index = pd.read_csv(
        path,
        sep=',',
        skiprows=1,
        header=None,
        names=["Date", "MSCI Australia", "MSCI Canada", "MSCI Europe", "MSCI Japan",
               "MSCI New Zealand", "MSCI Norway", "MSCI Sweden", "MSCI Switzerland",
               "MSCI UK", "MSCI USA"]
    )

    if index_name not in msci_eq_index.columns:
        raise ValueError(f"Index '{index_name}' not found in MSCI file.")

    msci_data = msci_eq_index[['Date', index_name]]
    return msci_data



def insert_full_asset(provider_name: str, asset_ticker: str, currency: str, df: pd.DataFrame):
    """
    Inserts asset time series data (e.g. MSCI indices) into the database.

    This version accepts a DataFrame with the original MSCI column name and
    renames it to match asset_ticker internally before processing.
    """
    with session:
        with session.begin():
            # 1. Provider
            provider = session.query(Provider).filter_by(name=provider_name).first()
            if not provider:
                provider = Provider(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Created new provider '{provider_name}' (ID {provider.provider_id})")
            else:
                print(f"Using existing provider '{provider_name}' (ID {provider.provider_id})")

            # 2. Asset
            asset = session.query(Asset).filter_by(asset_ticker=asset_ticker).first()
            if not asset:
                asset = Asset(asset_ticker=asset_ticker, currency=currency)
                session.add(asset)
                session.flush()
                print(f"Inserted new asset '{asset_ticker}'")
            else:
                print(f"Asset '{asset_ticker}' already exists")

            # 3. Asset_Ref
            asset_ref = session.query(Asset_Ref).filter_by(
                provider_id=provider.provider_id,
                asset_ticker=asset_ticker
            ).first()
            if not asset_ref:
                asset_ref = Asset_Ref(provider_id=provider.provider_id, asset_ticker=asset_ticker)
                session.add(asset_ref)
                session.flush()
                print(f"Created asset_ref (series_id: {asset_ref.series_id})")
            else:
                print(f"Asset_Ref already exists (series_id: {asset_ref.series_id})")

            # 4. Rename MSCI column to asset_ticker
            value_column = [col for col in df.columns if col != "Date"][0]
            if value_column != asset_ticker:
                df = df.rename(columns={value_column: asset_ticker})

            # 5. Clean DataFrame
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
            df = df[df["Date"].notna()]
            df = df[df[asset_ticker].notna()]

            # 6. Get existing dates
            existing_dates = session.query(Asset_TS.date).filter_by(series_id=asset_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            # 7. Prepare new rows
            new_rows = []
            for _, row in df.iterrows():
                if row["Date"] not in existing_dates_set:
                    new_rows.append({
                        "date": row["Date"],
                        "close": row[asset_ticker],
                        "series_id": asset_ref.series_id
                    })

            # 8. Insert
            if new_rows:
                session.bulk_insert_mappings(Asset_TS, new_rows)
                print(f"Inserted {len(new_rows)} new records into ASSET_TS.")
            else:
                print("No new asset time series data to insert.")

    print("Asset import completed.")



def insert_all_msci_assets(provider_name: str):
    """
    Lädt und speichert alle MSCI-Zeitreihen inklusive zugehöriger Währungen
    in die Asset-Datenbankstruktur (ASSET, ASSET_REF, ASSET_TS).
    """
    # Mapping: Indexname aus CSV → Währung
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



#insert_all_msci_assets(provider_name="bloomberg")

