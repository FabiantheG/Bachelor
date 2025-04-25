from database.models import *
from database.models import ASSET
from database.session import session
import pandas as pd


def csv_msci_bond_format(bond_label: str) -> pd.DataFrame:
    """
    Load MSCI government bond time series from a local CSV file.

    Reads a preformatted CSV containing daily 10-year government bond yields for various countries,
    normalizes column names by stripping "Note Generic Bid Yield", renames 'dt' to 'Date',
    and returns only 'Date' and the specified bond series.

    :param bond_label: Label of the bond series to extract (e.g., 'US Govt Bonds 10 Year').
    :type bond_label: str
    :raises ValueError: If `bond_label` is not found among the CSV columns.
    :return: DataFrame with columns ['Date', bond_label].
    :rtype: pandas.DataFrame
    """
    path = 'database/csv_file/msci_govt_10y_19991231_20250331_daily.csv.csv'
    msci_bond = pd.read_csv(path, sep=',')

    # Normalize column names
    new_columns = []
    for col in msci_bond.columns:
        if "Note Generic Bid Yield" in col:
            shortened = col.split("Note Generic Bid Yield")[0].strip()
            new_columns.append(shortened)
        else:
            new_columns.append(col)
    msci_bond.columns = new_columns

    # Rename 'dt' to 'Date'
    if 'dt' in msci_bond.columns:
        msci_bond.rename(columns={'dt': 'Date'}, inplace=True)

    # Verify bond_label exists
    if bond_label not in msci_bond.columns:
        raise ValueError(f"Bond '{bond_label}' not found in columns: {msci_bond.columns.tolist()}")

    return msci_bond[['Date', bond_label]]


def insert_full_asset(provider_name: str, asset_ticker: str, currency: str, df: pd.DataFrame):
    """
    Insert asset time series data into the database.

    Ensures the provider, ASSET and ASSET_REF entries exist; renames the DataFrame's value
    column to match `asset_ticker`; parses 'Date' to datetime.date; filters out missing values;
    and bulk-inserts only new (date, close, series_id) rows into ASSET_TS.

    :param provider_name: Name of the data provider (e.g., 'bloomberg').
    :type provider_name: str
    :param asset_ticker: Ticker symbol for the asset (e.g., 'US_Govt_Bonds_10_Year').
    :type asset_ticker: str
    :param currency: ISO currency code of the asset (e.g., 'USD').
    :type currency: str
    :param df: DataFrame with columns ['Date', <original series column>].
    :type df: pandas.DataFrame
    :raises ValueError: If the DataFrame lacks 'Date' or the series column after renaming.
    :return: None
    :rtype: None
    """
    with session:
        with session.begin():
            # 1) Provider
            provider = session.query(PROVIDER).filter_by(name=provider_name).first()
            if not provider:
                provider = PROVIDER(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Created new provider '{provider_name}' (ID {provider.provider_id})")
            else:
                print(f"Using existing provider '{provider_name}' (ID {provider.provider_id})")

            # 2) Asset
            asset = session.query(ASSET).filter_by(asset_ticker=asset_ticker).first()
            if not asset:
                asset = ASSET(asset_ticker=asset_ticker, currency=currency)
                session.add(asset)
                session.flush()
                print(f"Inserted new asset '{asset_ticker}'")
            else:
                print(f"Asset '{asset_ticker}' already exists")

            # 3) Asset_Ref
            asset_ref = session.query(ASSET_REF).filter_by(
                provider_id=provider.provider_id,
                asset_ticker=asset_ticker
            ).first()
            if not asset_ref:
                asset_ref = ASSET_REF(provider_id=provider.provider_id, asset_ticker=asset_ticker)
                session.add(asset_ref)
                session.flush()
                print(f"Created ASSET_REF (series_id: {asset_ref.series_id})")
            else:
                print(f"ASSET_REF already exists (series_id: {asset_ref.series_id})")

            # 4) Rename DataFrame column to asset_ticker
            if "Date" not in df.columns or len(df.columns) != 2:
                raise ValueError("DataFrame must contain exactly 'Date' and one series column.")
            value_column = [col for col in df.columns if col != "Date"][0]
            df = df.rename(columns={value_column: asset_ticker})

            # 5) Parse and clean DataFrame
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
            df = df[df["Date"].notna() & df[asset_ticker].notna()]

            # 6) Fetch existing dates
            existing = session.query(ASSET_TS.date).filter_by(series_id=asset_ref.series_id).all()
            existing_dates = {d[0] for d in existing}

            # 7) Prepare new rows
            new_rows = [
                {"date": row["Date"], "close": row[asset_ticker], "series_id": asset_ref.series_id}
                for _, row in df.iterrows()
                if row["Date"] not in existing_dates
            ]

            # 8) Bulk insert
            if new_rows:
                session.bulk_insert_mappings(ASSET_TS, new_rows)
                print(f"Inserted {len(new_rows)} new records into ASSET_TS.")
            else:
                print("No new asset time series data to insert.")

    print("Asset import completed.")


def insert_all_msci_bonds(provider_name: str):
    """
    Load and insert all MSCI government bond series into the database.

    Iterates over a predefined mapping of bond labels to currencies, uses
    `csv_msci_bond_format` to load each series, and calls `insert_full_asset`.
    Skips any series already fully loaded.

    :param provider_name: Name of the data provider (e.g., 'bloomberg').
    :type provider_name: str
    :return: None
    :rtype: None
    """
    bond_currency_map = {
        "Australia Govt Bonds 10 Year": "AUD",
        "Canadian Govt Bonds 10 Year": "CAD",
        "Germany Govt Bonds 10 Year": "EUR",
        "Japan Govt Bonds 10 Year": "JPY",
        "New Zealand Govt Bonds 10 Year": "NZD",
        "Norway Govt Bonds 10 Year": "NOK",
        "Swedish Govt Bonds 10 Year": "SEK",
        "Switzerland Govt Bonds 10 Year": "CHF",
        "UK Govt Bonds 10 Year": "GBP",
        "US Govt Bonds 10 Year": "USD"
    }

    for bond_label, currency in bond_currency_map.items():
        asset_ticker = bond_label.replace(" ", "_")
        try:
            print(f"\nProcessing: {bond_label} ({currency})")
            df = csv_msci_bond_format(bond_label)
            insert_full_asset(
                provider_name=provider_name,
                asset_ticker=asset_ticker,
                currency=currency,
                df=df
            )
        except Exception as e:
            print(f"Error while processing {bond_label}: {e}")
