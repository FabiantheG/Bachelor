from database.models import *
from database.models import GDP_RATES
from database.session import session
import pandas as pd

def csv_gdp_format(country):
    """
    Load GDP time series data for a specified country from a local CSV file and format dates.

    Reads the World Bank CSV of constant-2015-USD annual GDP figures, renames the first column
    to 'Date', filters to the given country column, parses 'Date' as datetime, and reformats
    it to 'DD.MM.YYYY'.

    :param country: Name of the country column to extract (e.g., 'Canada').
    :type country: str
    :return: DataFrame with columns ['Date', country], where 'Date' is formatted as 'DD.MM.YYYY'.
    :rtype: pandas.DataFrame
    :raises FileNotFoundError: If the CSV file cannot be found at the expected path.
    :raises KeyError: If `country` is not found among the CSV columns.
    """
    path = 'database/csv_file/gdp_const2015USD_worldbank_1974_2023_annual.csv'
    gdp_data = pd.read_csv(path, sep=',')

    # Rename first column to 'Date'
    gdp_data.columns.values[0] = 'Date'

    # Extract only 'Date' and the country column
    try:
        gdp = gdp_data[['Date', country]].copy()
    except KeyError:
        raise KeyError(f"Country '{country}' not found in CSV columns: {gdp_data.columns.tolist()}")

    # Parse and reformat Date
    gdp['Date'] = pd.to_datetime(gdp['Date'], errors='coerce').dt.strftime('%d.%m.%Y')
    return gdp


def insert_full_gdp(provider_name, country, df):
    """
    Insert GDP data into the database for a specific country.

    Ensures the provider exists (or creates it), registers the country in GDP_RATES,
    creates or reuses the GDP_REF and associated Economic_Indicator, and bulk-inserts
    any new (date, rate) pairs into GDP_TS, avoiding duplicates by date.

    :param provider_name: Name of the data provider (e.g., 'bloomberg').
    :type provider_name: str
    :param country: Country name matching the DataFrame column (e.g., 'Canada').
    :type country: str
    :param df: DataFrame with columns ['Date', country], dates as 'DD.MM.YYYY'.
    :type df: pandas.DataFrame
    :raises ValueError: If `df` lacks the 'Date' or country column.
    """
    with session:
        with session.begin():
            # Provider
            provider = session.query(PROVIDER).filter_by(name=provider_name).first()
            if not provider:
                provider = PROVIDER(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Provider '{provider_name}' created (ID: {provider.provider_id})")
            else:
                print(f"Provider '{provider_name}' already exists (ID: {provider.provider_id})")

            # Register country
            if not session.query(GDP_RATES).filter_by(country=country).first():
                session.add(GDP_RATES(country=country))
                print(f"Country '{country}' added to GDP_RATES.")
            else:
                print(f"Country '{country}' already exists in GDP_RATES.")

            # GDP_REF and Economic_Indicator
            gdp_ref = session.query(GDP_REF).filter_by(
                provider_id=provider.provider_id,
                country=country
            ).first()
            if not gdp_ref:
                gdp_ref = GDP_REF(provider_id=provider.provider_id, country=country)
                session.add(gdp_ref)
                session.flush()
                print(f"GDP_REF created (series_id: {gdp_ref.series_id})")
                ei = ECONOMIC_INDICATOR(series_id=gdp_ref.series_id, indicator_type='GDP')
                session.add(ei)
                print(f"Economic_Indicator entry created for GDP_REF (series_id: {gdp_ref.series_id})")
            else:
                print(f"GDP_REF already exists (series_id: {gdp_ref.series_id})")

            # Validate DataFrame
            if 'Date' not in df.columns or country not in df.columns:
                raise ValueError(f"DataFrame must contain columns 'Date' and '{country}'.")

            # Parse dates to datetime.date and drop invalid
            df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y', errors='coerce').dt.date
            df = df[df['Date'].notna() & df[country].notna()]

            # Fetch existing dates
            existing = session.query(GDP_TS.date).filter_by(series_id=gdp_ref.series_id).all()
            existing_dates = {d[0] for d in existing}

            # Build new records, skipping duplicates
            new_records = [
                {'date': row['Date'], 'rate': row[country], 'series_id': gdp_ref.series_id}
                for _, row in df.iterrows()
                if row['Date'] not in existing_dates
            ]

            if new_records:
                session.bulk_insert_mappings(GDP_TS, new_records)
                print(f"Inserted {len(new_records)} new GDP_TS records.")
            else:
                print("No new GDP_TS records to insert – all dates already exist.")

    print(f"Finished GDP import for {country}.")


def insert_all_gdp():
    """
    Load and insert GDP data for a predefined list of countries.

    Uses 'bloomberg' as the provider, iterates a fixed country list, calls
    `csv_gdp_format` then `insert_full_gdp`, and logs progress/errors.

    :return: None
    """
    countries = [
        "Australia", "Canada", "Switzerland", "Germany", "Euro area", "Spain",
        "France", "Italy", "Japan", "Norway", "New Zealand", "Sweden", "United States"
    ]
    provider = "bloomberg"

    for country in countries:
        try:
            print(f"\nProcessing GDP data for {country}...")
            df = csv_gdp_format(country)
            insert_full_gdp(provider_name=provider, country=country, df=df)
        except Exception as e:
            print(f"Error processing {country}: {e}")
