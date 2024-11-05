import requests
import pandas as pd
import psycopg2
import os

API_URL = "https://data.sfgov.org/resource/wr8u-xric.json"
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "fire_db")
DB_USER = os.getenv("POSTGRES_USER", "db_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "db_password")
LIMIT = 1000  

def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS public.staging_fire_incidents (
        incident_number TEXT,
        exposure_number TEXT,
        id TEXT,
        address TEXT,
        incident_date TEXT,
        call_number TEXT,
        alarm_dttm TEXT,
        arrival_dttm TEXT,
        close_dttm TEXT,
        city TEXT,
        zipcode TEXT,
        battalion TEXT,
        station_area TEXT,
        box TEXT,
        suppression_units TEXT,
        suppression_personnel TEXT,
        ems_units TEXT,
        ems_personnel TEXT,
        other_units TEXT,
        other_personnel TEXT,
        first_unit_on_scene TEXT,
        estimated_property_loss TEXT,
        estimated_contents_loss TEXT,
        fire_fatalities TEXT,
        fire_injuries TEXT,
        civilian_fatalities TEXT,
        civilian_injuries TEXT,
        number_of_alarms TEXT,
        primary_situation TEXT,
        mutual_aid TEXT,
        action_taken_primary TEXT,
        action_taken_secondary TEXT,
        action_taken_other TEXT,
        detector_alerted_occupants TEXT,
        property_use TEXT,
        area_of_fire_origin TEXT,
        ignition_cause TEXT,
        ignition_factor_primary TEXT,
        ignition_factor_secondary TEXT,
        heat_source TEXT,
        item_first_ignited TEXT,
        human_factors_associated_with_ignition TEXT,
        structure_type TEXT,
        structure_status TEXT,
        floor_of_fire_origin TEXT,
        fire_spread TEXT,
        no_flame_spread TEXT,
        number_of_floors_with_minimum_damage TEXT,
        number_of_floors_with_significant_damage TEXT,
        number_of_floors_with_heavy_damage TEXT,
        number_of_floors_with_extreme_damage TEXT,
        detectors_present TEXT,
        detector_type TEXT,
        detector_operation TEXT,
        detector_effectiveness TEXT,
        detector_failure_reason TEXT,
        automatic_extinguishing_system_present TEXT,
        automatic_extinguishing_system_type TEXT,
        automatic_extinguishing_system_performance TEXT,
        automatic_extinguishing_system_failure_reason TEXT,
        number_of_sprinkler_heads_operating TEXT,
        supervisor_district TEXT,
        neighborhood_district TEXT,
        point TEXT,  
        data_as_of TEXT,
        data_loaded_at TEXT
    );
    """

    with psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
    print("Table staging_fire_incidents created (if it didn't exist already).")

def fetch_and_load_data():
    offset = 0

    while True:
        response = requests.get(f"{API_URL}?$limit={LIMIT}&$offset={offset}")
        response.raise_for_status()
        data = response.json()

        if not data:
            print("No more data to process.")
            break
        
        df = pd.json_normalize(data)

        if "point.coordinates" in df.columns:
            df['point'] = df['point.coordinates'].apply(convert_to_point_geom)
            df.drop(columns=[col for col in df.columns if col.startswith('point.') and col != 'point'], inplace=True)

        load_data_to_postgres(df)
        
        if len(data) < LIMIT:
            print("End of data reached for daily load.")
            break

        offset += LIMIT
        print(f"Fetched and inserted {len(data)} records, total offset: {offset}")

def convert_to_point_geom(coordinates):
    if isinstance(coordinates, list) and len(coordinates) == 2:
        lon, lat = coordinates
        return f'POINT({lon} {lat})'
    return None

def load_data_to_postgres(df):
    try:
        with psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as conn:
            with conn.cursor() as cursor:
                columns_sql = ', '.join([f'"{col}"' for col in df.columns])
                values_sql = ', '.join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO staging_fire_incidents ({columns_sql}) VALUES ({values_sql});"
                
                for _, row in df.iterrows():
                    values = [str(row[col]) if pd.notnull(row[col]) else None for col in df.columns]
                    cursor.execute(insert_query, values)

                conn.commit()
    except Exception as e:
        pass

if __name__ == "__main__":
    create_table()           
    fetch_and_load_data()   
