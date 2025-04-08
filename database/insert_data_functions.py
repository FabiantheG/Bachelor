from database.session import session
from database.models import *
from functions import *
import pandas as pd
from sqlalchemy import and_


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

def insert_fx(path,provider,duration):
    data = pd.read_csv(path) # get the data from the csv file
    quote = path[-7:-4] #get the quote from the name
    base = path[-10:-7] #get the base



    if duration == '1M':
        # calculate the forward rate depending on the quotation
        if base == 'JPY' or quote == 'JPY':
            if base == 'EUR' or quote == 'EUR':
                data['forward'] = data['spot'] - data['px'] / 1000
            data['forward'] = data['spot'] - data['px'] / 100
        else:
            data['forward'] = data['spot'] + data['px'] / 10000

        # create the rate
        data['rate'] = data['forward']
    else: # if duration is not a forward rate, we use the spotrate
        data['rate'] = data['spot']



    with session:
        with session.begin():
            existing_fx = session.query(FX_Rates).filter(
                and_(
                    FX_Rates.base_cur == base,
                    FX_Rates.quote_cur == quote,
                    FX_Rates.duration == duration
                )
            ).first()

            if not existing_fx:


                fx =  FX_Rates(base_cur = base , quote_cur=quote,duration=duration)
                session.add(fx)

            ref = FX_Ref(provider_id=provider, base_cur = base , quote_cur=quote,duration=duration)
            session.add(ref)
            session.flush()  # sorgt dafür, dass ref.series_id generiert und verfügbar wird


            # Erstelle eine Liste von Mappings (Dictionaries) für den Bulk Insert
            asset_ts_mappings = []
            for _, row in data.iterrows():
                asset_ts_mappings.append({
                    "date": row['Date'],     # Datum aus der DataFrame-Spalte 'Date'
                    "rate": row['rate'],   # Schlusskurs aus der Spalte 'Close'
                    "series_id": ref.series_id
                })
            session.bulk_insert_mappings(FX_TS, asset_ts_mappings)

def insert_fx(path,provider,duration):
    data = pd.read_csv(path) # get the data from the csv file
    quote = path[-7:-4] #get the quote from the name
    base = path[-10:-7] #get the base



    if duration == '1M':
        # calculate the forward rate depending on the quotation
        if base == 'JPY' or quote == 'JPY':
            if base == 'EUR' or quote == 'EUR':
                data['forward'] = data['spot'] - data['px'] / 1000
            data['forward'] = data['spot'] - data['px'] / 100
        else:
            data['forward'] = data['spot'] + data['px'] / 10000

        # create the rate
        data['rate'] = data['forward']
    else: # if duration is not a forward rate, we use the spotrate
        data['rate'] = data['spot']



    with session:
        with session.begin():
            existing_fx = session.query(FX_Rates).filter(
                and_(
                    FX_Rates.base_cur == base,
                    FX_Rates.quote_cur == quote,
                    FX_Rates.duration == duration
                )
            ).first()

            if not existing_fx:


                fx =  FX_Rates(base_cur = base , quote_cur=quote,duration=duration)
                session.add(fx)

            ref = FX_Ref(provider_id=provider, base_cur = base , quote_cur=quote,duration=duration)
            session.add(ref)
            session.flush()  # sorgt dafür, dass ref.series_id generiert und verfügbar wird


            # Erstelle eine Liste von Mappings (Dictionaries) für den Bulk Insert
            asset_ts_mappings = []
            for _, row in data.iterrows():
                asset_ts_mappings.append({
                    "date": row['Date'],     # Datum aus der DataFrame-Spalte 'Date'
                    "rate": row['rate'],   # Schlusskurs aus der Spalte 'Close'
                    "series_id": ref.series_id
                })
            session.bulk_insert_mappings(FX_TS, asset_ts_mappings)



insert_yahoo_db("AAPL", start="2025-01-05", end="2025-01-15")
insert_fx('AUDUSD.csv',1,'1M')

list = ['USDCHF','USDSEK','USDNOK','USDAUD','USDNZD','USDGBP','USDEUR','USDJPY','USDCAD','CHFUSD','SEKUSD','NOKUSD','AUDUSD','NZDUSD','GBPUSD','EURUSD','JPYUSD','CADUSD']




