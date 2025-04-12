

from models.provider import Provider
from models.gdp import GDP_Rates, GDP_Ref, GDP_TS
from models.economic_indicator import Economic_Indicator
from session import session
import pandas as pd



def csv_gdp_format(country):
    """
    Loads GDP time series data for a specified country from a local CSV file.
    Formats the 'Date' column from YYYY-MM-DD to DD.MM.YYYY.
    """
    path = '/Users/lukas/Documents/Bachelor/database/csv_file/gdp_const2015USD_worldbank_1974_2023_annual.csv'
    gdp_data = pd.read_csv(path, sep=',')

    # Rename first column to 'Date'
    gdp_data.columns.values[0] = 'Date'

    # Keep only 'Date' and selected country
    gdp = gdp_data[['Date', country]].copy()

    # Convert to datetime if not already
    gdp["Date"] = pd.to_datetime(gdp["Date"], errors='coerce')

    # Format as 'DD.MM.YYYY'
    gdp["Date"] = gdp["Date"].dt.strftime('%d.%m.%Y')

    return gdp

#print(csv_gdp_format('Canada'))



def insert_full_gdp(provider_name: str, country: str, df: pd.DataFrame):
    """
    Inserts GDP data for a specific country into the database.
    Expects a DataFrame with columns ['Date', country] (e.g., ['Date', 'Canada']).
    """
    with session:
        with session.begin():
            # Step 1: Provider
            provider = session.query(Provider).filter_by(name=provider_name).first()
            if not provider:
                provider = Provider(name=provider_name)
                session.add(provider)
                session.flush()
                print(f"Provider '{provider_name}' created (ID: {provider.provider_id})")
            else:
                print(f"Provider '{provider_name}' already exists (ID: {provider.provider_id})")

            # Step 2: GDP_RATES
            if not session.query(GDP_Rates).filter_by(country=country).first():
                session.add(GDP_Rates(country=country))
                print(f"Country '{country}' added to GDP_RATES.")
            else:
                print(f"Country '{country}' already exists in GDP_RATES.")

            # Step 3: GDP_REF + Economic_Indicator
            gdp_ref = session.query(GDP_Ref).filter_by(
                provider_id=provider.provider_id,
                country=country
            ).first()

            if not gdp_ref:
                gdp_ref = GDP_Ref(provider_id=provider.provider_id, country=country)
                session.add(gdp_ref)
                session.flush()
                print(f"GDP_REF created (series_id: {gdp_ref.series_id})")

                ei = Economic_Indicator(series_id=gdp_ref.series_id, indicator_type='GDP')
                session.add(ei)
                print(f"Economic_Indicator entry created for GDP_REF (series_id: {gdp_ref.series_id})")
            else:
                print(f"GDP_REF already exists (series_id: {gdp_ref.series_id})")

            # Step 4: Validate DataFrame columns
            if "Date" not in df.columns or country not in df.columns:
                raise ValueError(f"DataFrame must contain columns 'Date' and '{country}'.")

            # Step 5: Format and clean date and rate values
            df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y", errors="coerce").dt.date
            df = df[df["Date"].notna() & df[country].notna()]

            # Step 6: Filter only new dates
            existing_dates = session.query(GDP_TS.date).filter_by(series_id=gdp_ref.series_id).all()
            existing_dates_set = {d[0] for d in existing_dates}

            new_records = [
                {"date": row["Date"], "rate": row[country], "series_id": gdp_ref.series_id}
                for _, row in df.iterrows() if row["Date"] not in existing_dates_set
            ]

            # Step 7: Insert new records
            if new_records:
                session.bulk_insert_mappings(GDP_TS, new_records)
                print(f"Inserted {len(new_records)} new GDP_TS records.")
            else:
                print("No new GDP_TS records to insert – all dates already exist.")

    print(f"Finished GDP import for {country}.\n")

# insert_full_gdp(provider_name='bloomberg', country='Canada', df=csv_gdp_format('Canada'))



def insert_all_gdp():
    """
    Automatically reads and inserts GDP data for a fixed list of countries
    using the insert_full_gdp function. Uses 'bloomberg' as provider.
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

#insert_all_gdp()

