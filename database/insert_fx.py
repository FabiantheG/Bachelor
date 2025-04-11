
from models.provider import Provider
from models.fx import FX_Rates, FX_Ref, FX_TS
from session import session
import pandas as pd

# -------- FX Spot -----------

def save_spot_price_csv():
    # CSV-Datei einlesen
    csv_path = '/Users/lukas/Documents/Bachelor/database/csv_file/fx_fwd_pts_vs_usd_ds_19991231_20250331_daily.csv'
    df = pd.read_csv(csv_path, sep=';', skiprows=1)

    # Extrahiere die erste Spalte (angenommen, diese enthält das Datum oder den Code)
    df_date = df.iloc[:, 0].copy()
    df_date.name = 'Date'

    # Auswahl der Spalten 37 bis 45 (9 Spalten) und Kopie erstellen, um SettingWithCopyWarning zu vermeiden
    df_subset = df[df.columns[37:46]].copy()


    # Neue Spaltennamen definieren für den Subset
    neue_namen = ['Spot_CHF_TO_USD', 'Spot_EUR_TO_USD', 'Spot_CAD_TO_USD',
                  'Spot_AUD_TO_USD', 'Spot_NZD_TO_USD', 'Spot_GBP_TO_USD',
                  'Spot_JPY_TO_USD', 'Spot_NOK_TO_USD', 'Spot_SEK_TO_USD']
    df_subset.columns = neue_namen

    # Kombiniere das Datum bzw. den Code mit dem Subset – Datum (erste Spalte) wird beibehalten
    df_final = pd.concat([df_date, df_subset], axis=1)


    # Ausgabe-Pfad definieren
    output_path = '/Users/lukas/Documents/Bachelor/database/csv_file/spot_price.csv'

    # Speichern des finalen DataFrames als CSV
    df_final.to_csv(output_path, sep=';', index=False)

    # Rückgabe der Spaltennamen als Kontrolle
    return df_final

#print(save_spot_price_csv())


def csv_spot_price_format(spot_label: str) -> pd.DataFrame:
    """
    Loads the spot price time series along with the associated date from the CSV file
    and returns only two columns: 'Date' and the desired spot price label.

    Parameters:
    -----------
    spot_label : str
        The desired spot price label, e.g., 'Spot_CHF_TO_USD'. This must be present as a column name in the CSV file.

    Returns:
    --------
    pd.DataFrame with two columns: ['Date', spot_label]
    """
    path = '/Users/lukas/Documents/Bachelor/database/csv_file/spot_price.csv'
    spot_price_df = pd.read_csv(path, sep=';')

    # Ensure that the 'Date' column exists
    if 'Date' not in spot_price_df.columns:
        raise ValueError(
            f"The 'Date' column was not found in the CSV file. Found columns: {spot_price_df.columns.tolist()}"
        )

    # Ensure that the requested spot label is present
    if spot_label not in spot_price_df.columns:
        raise ValueError(
            f"Spot label '{spot_label}' not found. Available columns: {spot_price_df.columns.tolist()}"
        )

    # Return only the 'Date' and the desired spot label columns
    return spot_price_df[['Date', spot_label]]

# Example call:
#df = csv_spot_price_format('Spot_CHF_TO_USD')
#print(df.head())

def insert_full_fx(provider_name: str, duration: str, df: pd.DataFrame):
    """
    Inserts FX spot data into the database:
    - Ensures provider exists.
    - Extracts currency pair from DataFrame column title (format: Spot_BASE_TO_QUOTE).
    - Ensures FX_REF exists for the provider/currency pair and duration.
    - Inserts only new records into FX_TS (no duplicates by date).
    - Parses date column from DD.MM.YYYY format.

    Parameters:
    -----------
    provider_name : str
        Name of the data provider (e.g., 'bloomberg').

    duration : str
        The duration type (e.g., 'Spot', 'Daily').

    df : pd.DataFrame
        Must contain columns ['Date', 'Spot_BASE_TO_QUOTE'], with Date as a string in DD.MM.YYYY.
    """
    spot_cols = [col for col in df.columns if col.startswith("Spot_")]
    if len(spot_cols) != 1:
        raise ValueError("DataFrame must contain exactly one 'Spot_BASE_TO_QUOTE' column.")

    spot_label = spot_cols[0]
    _, base_cur, _, quote_cur = spot_label.split('_')

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

            # Step 2: FX_RATES
            if not session.query(FX_Rates).filter_by(base_cur=base_cur, quote_cur=quote_cur, duration=duration).first():
                session.add(FX_Rates(base_cur=base_cur, quote_cur=quote_cur, duration=duration))
                print(f"FX currency pair '{base_cur}/{quote_cur}' with duration '{duration}' inserted into FX_RATES.")
            else:
                print(f"FX currency pair '{base_cur}/{quote_cur}' with duration '{duration}' already exists in FX_RATES.")

            # Step 3: FX_REF
            fx_ref = session.query(FX_Ref).filter_by(
                provider_id=provider.provider_id,
                base_cur=base_cur,
                quote_cur=quote_cur,
                duration=duration
            ).first()
            if not fx_ref:
                fx_ref = FX_Ref(provider_id=provider.provider_id, base_cur=base_cur, quote_cur=quote_cur, duration=duration)
                session.add(fx_ref)
                session.flush()
                print(f"FX_REF created (series_id: {fx_ref.series_id})")
            else:
                print(f"FX_REF already exists (series_id: {fx_ref.series_id})")

            # Step 4: Validate DataFrame columns
            if "Date" not in df.columns:
                raise ValueError("DataFrame must contain a 'Date' column.")

            # Step 5: Convert 'Date' to datetime.date
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date

            # Remove rows with invalid or missing date or rate
            df = df[df["Date"].notna() & df[spot_label].notna()]

            # Step 6: Get existing dates from DB
            existing_dates = session.query(FX_TS.date).filter_by(series_id=fx_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            # Step 7: Prepare only new entries
            new_records = [
                {"date": row["Date"], "rate": row[spot_label], "series_id": fx_ref.series_id}
                for _, row in df.iterrows() if row["Date"] not in existing_dates_set
            ]

            # Step 8: Insert if needed
            if new_records:
                session.bulk_insert_mappings(FX_TS, new_records)
                print(f"Inserted {len(new_records)} new FX_TS records.")
            else:
                print("No new FX_TS records to insert – all dates already exist.")

    print("FX import completed.")



#insert_full_fx(provider_name='LSEG DS', duration='Spot', df=csv_spot_price_format('Spot_CHF_TO_USD'))


def insert_all_spot_data():
    # List of spot price pairs.
    spot_pairs = [
        'Spot_CHF_TO_USD',
        'Spot_EUR_TO_USD',
        'Spot_CAD_TO_USD',
        'Spot_AUD_TO_USD',
        'Spot_NZD_TO_USD',
        'Spot_GBP_TO_USD',
        'Spot_JPY_TO_USD',
        'Spot_NOK_TO_USD',
        'Spot_SEK_TO_USD'
    ]
    # Corresponding provider names.
    provider_list = [
        'LSEG DS', 'RFV', 'LSEG DS', 'LSEG DS', 'LSEG DS', 'LSEG DS', 'LSEG DS', 'WMR', 'WMR' ]

    # Iterate through both lists simultaneously.
    for spot_label, provider_name in zip(spot_pairs, provider_list):
        print(f"Inserting data for {spot_label} provided by {provider_name}...")
        # Retrieve the DataFrame for the given spot label
        df = csv_spot_price_format(spot_label)
        # Call the insert function with the provider_name, fixed duration "Spot", and the DataFrame.
        insert_full_fx(provider_name=provider_name, duration='Spot', df=df)
        print(f"Finished inserting data for {spot_label} provided by {provider_name}.\n")



# insert_all_spot_data()


# ----------- Forward Points ---------------


import pandas as pd


def save_forward_points_subset():
    # CSV-Datei einlesen
    csv_path = '/Users/lukas/Documents/Bachelor/database/csv_file/fx_fwd_pts_vs_usd_ds_19991231_20250331_daily.csv'
    df = pd.read_csv(csv_path, sep=';', skiprows=1)

    # Wähle die ersten 37 Spalten aus (Index 0 bis 36)
    df_subset = df[df.columns[0:37]]

    # Definiere die neuen Spaltennamen
    names = [
        'Date',
        'USD_TO_AUD_1M_FWD_PTS', 'USD_TO_AUD_3M_FWD_PTS', 'USD_TO_AUD_6M_FWD_PTS',
        'USD_TO_AUD_9M_FWD_PTS', 'USD_TO_EUR_1M_FWD_PTS', 'USD_TO_EUR_3M_FWD_PTS',
        'USD_TO_EUR_6M_FWD_PTS', 'USD_TO_EUR_9M_FWD_PTS', 'USD_TO_NZD_1M_FWD_PTS',
        'USD_TO_NZD_3M_FWD_PTS', 'USD_TO_NZD_6M_FWD_PTS', 'USD_TO_NZD_9M_FWD_PTS',
        'SEK_TO_USD_1M_FWD_PTS', 'SEK_TO_USD_3M_FWD_PTS', 'SEK_TO_USD_6M_FWD_PTS',
        'SEK_TO_USD_9M_FWD_PTS', 'NOK_TO_USD_1M_FWD_PTS', 'NOK_TO_USD_3M_FWD_PTS',
        'NOK_TO_USD_6M_FWD_PTS', 'NOK_TO_USD_9M_FWD_PTS', 'USD_TO_GBP_1M_FWD_PTS',
        'USD_TO_GBP_3M_FWD_PTS', 'USD_TO_GBP_6M_FWD_PTS', 'USD_TO_GBP_9M_FWD_PTS',
        'JPY_TO_USD_1M_FWD_PTS', 'JPY_TO_USD_3M_FWD_PTS', 'JPY_TO_USD_6M_FWD_PTS',
        'JPY_TO_USD_9M_FWD_PTS', 'CAD_TO_USD_1M_FWD_PTS', 'CAD_TO_USD_3M_FWD_PTS',
        'CAD_TO_USD_6M_FWD_PTS', 'CAD_TO_USD_9M_FWD_PTS', 'CHF_TO_USD_1M_FWD_PTS',
        'CHF_TO_USD_3M_FWD_PTS', 'CHF_TO_USD_6M_FWD_PTS', 'CHF_TO_USD_9M_FWD_PTS'
    ]

    # Setze die neuen Spaltennamen
    df_subset.columns = names

    # Definiere den Ausgabe-Pfad
    output_path = '/Users/lukas/Documents/Bachelor/database/csv_file/forward_points.csv'

    # Speichere das DataFrame als neues CSV
    df_subset.to_csv(output_path, sep=';', index=False)

    # Gib die Spaltennamen zurück
    return df_subset


#print(save_forward_points_subset())

import pandas as pd


def csv_forward_points_format(forward_label: str):
    """
    Loads the forward points time series along with the associated date from the CSV file
    and returns only two columns: 'Date' and the specified forward points column.

    Parameters:
    -----------
    forward_label : str
        The desired forward points label, e.g., 'USD_TO_AUD_1M_FWD_PTS'. This must be present as a column name in the CSV file.

    Returns:
    --------
    pd.DataFrame
        A DataFrame with two columns: ['Date', forward_label] with 'Date' converted to datetime.
    """
    file_path = '/Users/lukas/Documents/Bachelor/database/csv_file/forward_points.csv'

    # Read the CSV file
    forward_points_df = pd.read_csv(file_path, sep=';')

    # Check if the 'Date' column exists
    if 'Date' not in forward_points_df.columns:
        raise ValueError(
            f"The 'Date' column was not found in the CSV file. Found columns: {forward_points_df.columns.tolist()}"
        )

    # Check if the specified forward points label exists
    if forward_label not in forward_points_df.columns:
        raise ValueError(
            f"Forward points label '{forward_label}' not found. Available columns: {forward_points_df.columns.tolist()}"
        )

    # Convert the 'Date' column to datetime format with dayfirst=True to avoid parsing warnings
    try:
        forward_points_df['Date'] = pd.to_datetime(forward_points_df['Date'], dayfirst=True)
    except Exception as e:
        raise ValueError(f"Error converting 'Date' column to datetime: {e}")

    # Return only the 'Date' and the specified forward points label columns
    return forward_points_df[['Date', forward_label]]


# Example usage:
#df = csv_forward_points_format('CHF_TO_USD_6M_FWD_PTS')
#print(df.head())


def insert_fx_forward(provider_name: str, df: pd.DataFrame):
    """
    Inserts FX forward points data into the database:
      - Ensures the provider exists.
      - Extracts the currency pair and duration from the DataFrame column title. Expected format:
        BASE_TO_QUOTE_<duration>_FWD_PTS (e.g., 'CHF_TO_USD_6M_FWD_PTS').
      - Ensures FX_Rates exists for the provider/currency pair and extracted duration.
      - Ensures FX_Ref exists for the provider/currency pair and duration.
      - Inserts only new records into FX_TS (no duplicate dates).
      - Parses the Date column from DD.MM.YYYY format.

    Parameters:
    -----------
    provider_name : str
        Name of the data provider (e.g., 'LSEG DS').
    df : pd.DataFrame
        A DataFrame containing columns ['Date', '<BASE>_TO_<QUOTE>_<duration>_FWD_PTS'], with Date as a string in DD.MM.YYYY.
    """

    # Identify the forward points column.
    fwd_cols = [col for col in df.columns if col.endswith("_FWD_PTS")]
    if len(fwd_cols) != 1:
        raise ValueError("DataFrame must contain exactly one forward points column in the format 'BASE_TO_QUOTE_<duration>_FWD_PTS'.")
    forward_label = fwd_cols[0]

    # Extract base and quote currencies and duration from the column title.
    tokens = forward_label.split('_')
    if len(tokens) < 5 or tokens[1] != "TO":
        raise ValueError(f"Forward points column name '{forward_label}' does not match the expected format.")
    base_cur = tokens[0]
    quote_cur = tokens[2]
    duration = tokens[3]  # Extracting duration dynamically

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

            # Step 2: FX_Rates
            fx_rate_exists = session.query(FX_Rates).filter_by(base_cur=base_cur, quote_cur=quote_cur, duration=duration).first()
            if not fx_rate_exists:
                session.add(FX_Rates(base_cur=base_cur, quote_cur=quote_cur, duration=duration))
                print(f"FX currency pair '{base_cur}/{quote_cur}' with duration '{duration}' inserted into FX_Rates.")
            else:
                print(f"FX currency pair '{base_cur}/{quote_cur}' with duration '{duration}' already exists in FX_Rates.")

            # Step 3: FX_REF
            fx_ref = session.query(FX_Ref).filter_by(
                provider_id=provider.provider_id,
                base_cur=base_cur,
                quote_cur=quote_cur,
                duration=duration
            ).first()
            if not fx_ref:
                fx_ref = FX_Ref(provider_id=provider.provider_id, base_cur=base_cur, quote_cur=quote_cur, duration=duration)
                session.add(fx_ref)
                session.flush()
                print(f"FX_REF created (series_id: {fx_ref.series_id})")
            else:
                print(f"FX_REF already exists (series_id: {fx_ref.series_id})")

            # Step 4: Validate DataFrame columns
            if "Date" not in df.columns:
                raise ValueError("DataFrame must contain a 'Date' column.")

            # Step 5: Convert 'Date' column to datetime.date using the provided format
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date
            df = df[df["Date"].notna() & df[forward_label].notna()]

            # Step 6: Get existing dates from DB for the series
            existing_dates = session.query(FX_TS.date).filter_by(series_id=fx_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            # Step 7: Prepare only new entries
            new_records = [
                {"date": row["Date"], "rate": row[forward_label], "series_id": fx_ref.series_id}
                for _, row in df.iterrows() if row["Date"] not in existing_dates_set
            ]

            # Step 8: Insert new records if available
            if new_records:
                session.bulk_insert_mappings(FX_TS, new_records)
                print(f"Inserted {len(new_records)} new FX_TS records.")
            else:
                print("No new FX_TS records to insert – all dates already exist.")

    print("FX forward points import completed.")

# insert_fx_forward(provider_name='bloomberg', df=csv_forward_points_format('CHF_TO_USD_6M_FWD_PTS'))




def insert_all_fx_forward():
    names = [
        'USD_TO_AUD_1M_FWD_PTS', 'USD_TO_AUD_3M_FWD_PTS', 'USD_TO_AUD_6M_FWD_PTS',
        'USD_TO_AUD_9M_FWD_PTS', 'USD_TO_EUR_1M_FWD_PTS', 'USD_TO_EUR_3M_FWD_PTS',
        'USD_TO_EUR_6M_FWD_PTS', 'USD_TO_EUR_9M_FWD_PTS', 'USD_TO_NZD_1M_FWD_PTS',
        'USD_TO_NZD_3M_FWD_PTS', 'USD_TO_NZD_6M_FWD_PTS', 'USD_TO_NZD_9M_FWD_PTS',
        'SEK_TO_USD_1M_FWD_PTS', 'SEK_TO_USD_3M_FWD_PTS', 'SEK_TO_USD_6M_FWD_PTS',
        'SEK_TO_USD_9M_FWD_PTS', 'NOK_TO_USD_1M_FWD_PTS', 'NOK_TO_USD_3M_FWD_PTS',
        'NOK_TO_USD_6M_FWD_PTS', 'NOK_TO_USD_9M_FWD_PTS', 'USD_TO_GBP_1M_FWD_PTS',
        'USD_TO_GBP_3M_FWD_PTS', 'USD_TO_GBP_6M_FWD_PTS', 'USD_TO_GBP_9M_FWD_PTS',
        'JPY_TO_USD_1M_FWD_PTS', 'JPY_TO_USD_3M_FWD_PTS', 'JPY_TO_USD_6M_FWD_PTS',
        'JPY_TO_USD_9M_FWD_PTS', 'CAD_TO_USD_1M_FWD_PTS', 'CAD_TO_USD_3M_FWD_PTS',
        'CAD_TO_USD_6M_FWD_PTS', 'CAD_TO_USD_9M_FWD_PTS', 'CHF_TO_USD_1M_FWD_PTS',
        'CHF_TO_USD_3M_FWD_PTS', 'CHF_TO_USD_6M_FWD_PTS', 'CHF_TO_USD_9M_FWD_PTS'
    ]

    # Replace 'your_provider_name' with the actual provider if different for each currency pair,
    # you can also create a dictionary to map each pair to a provider if necessary.
    provider_name = 'bloomberg'

    # Iterate over all columns except the first one ('Date').
    for forward_label in names[1:]:
        print(f"Inserting data for {forward_label}...")
        df = csv_forward_points_format(forward_label)
        insert_fx_forward(provider_name=provider_name, df=df)
        print(f"Finished inserting data for {forward_label}.\n")


#insert_all_fx_forward()