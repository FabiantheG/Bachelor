from SQL_Alchemy import *
from functions import *

def insert_yahoo_db(ticker, start, end):
    provider = 1
    data = get_asset(ticker, start, end)


    with session:
        with session.begin():
            existing_asset = session.query(Asset).filter_by(asset_ticker=ticker).first()

            if not existing_asset:
                yfinance = yf.Ticker(ticker)
                currency = yfinance.info.get('currency')
                asset =  Asset(currency = currency , asset_ticker=ticker)
                session.add(asset)

            ref = Asset_Ref(provider_id=provider, asset_ticker=ticker)
            session.add(ref)
            session.flush()  # sorgt dafür, dass ref.series_id generiert und verfügbar wird


            # Erstelle eine Liste von Mappings (Dictionaries) für den Bulk Insert
            asset_ts_mappings = []
            for _, row in data.iterrows():
                asset_ts_mappings.append({
                    "date": row['Date'],     # Datum aus der DataFrame-Spalte 'Date'
                    "close": row['Close'],   # Schlusskurs aus der Spalte 'Close'
                    "series_id": ref.series_id
                })

            # Führe den Bulk Insert in die Tabelle ASSET_TS aus
            session.bulk_insert_mappings(Asset_TS, asset_ts_mappings)


insert_yahoo_db("AAPL", start="2025-01-05", end="2025-01-15")








