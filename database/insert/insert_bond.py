from database.models import *
from database.models import Asset
from database.session import session
import pandas as pd


def csv_msci_bond_format(bond_label: str) -> pd.DataFrame:
    """
    Lädt Bond-Zeitreihe mit zugehörigem Datum aus CSV und gibt nur zwei Spalten zurück:
    'Date' und die gewünschte Bond-Zeitreihe.

    Parameter:
    -----------
    bond_label : str
        Der Bond-Name, z. B. 'US Govt Bonds 10 Year' (gekürzt wie in Spalten nach dem Einlesen)

    Rückgabe:
    ---------
    pd.DataFrame mit zwei Spalten: ['Date', bond_label]
    """
    path = 'database/csv_file/msci_govt_10y_19991231_20250331_daily.csv.csv'
    msci_bond = pd.read_csv(path, sep=',')

    # Spaltennamen kürzen
    new_columns = []
    for col in msci_bond.columns:
        if "Note Generic Bid Yield" in col:
            shortened = col.split("Note Generic Bid Yield")[0].strip()
            new_columns.append(shortened)
        else:
            new_columns.append(col)

    msci_bond.columns = new_columns

    # Rename dt → Date
    if 'dt' in msci_bond.columns:
        msci_bond.rename(columns={'dt': 'Date'}, inplace=True)

    # Sicherstellen, dass gewünschter Bond existiert
    if bond_label not in msci_bond.columns:
        raise ValueError(f"Bond '{bond_label}' not found in columns: {msci_bond.columns.tolist()}")

    # Nur Date + gewünschte Bond-Spalte zurückgeben
    return msci_bond[['Date', bond_label]]


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


def insert_all_msci_bonds(provider_name: str):
    """
    Lädt und speichert alle MSCI-Zeitreihen inklusive zugehöriger Währungen
    in die Asset-Datenbankstruktur (ASSET, ASSET_REF, ASSET_TS).
    """
    # Mapping: Indexname aus CSV → Währung
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

    for index_name, currency in bond_currency_map.items():
        asset_ticker = index_name.replace(" ", "_")

        try:
            print(f"\nProcessing: {index_name} ({currency})")
            df = csv_msci_bond_format(index_name)
            insert_full_asset(
                provider_name=provider_name,
                asset_ticker=asset_ticker,
                currency=currency,
                df=df
            )
        except Exception as e:
            print(f"Error while processing {index_name}: {e}")


#insert_all_msci_bonds(provider_name="bloomberg")




