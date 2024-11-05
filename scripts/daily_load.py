import requests
import pandas as pd
import psycopg2
import os
from datetime import datetime, timedelta

API_URL = "https://data.sfgov.org/resource/wr8u-xric.json"
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "fire_db")
DB_USER = os.getenv("POSTGRES_USER", "db_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "db_password")
LIMIT = 1000

def convert_to_point_geom(coordinates):
    if isinstance(coordinates, list) and len(coordinates) == 2:
        lon, lat = coordinates
        return f'POINT({lon} {lat})'
    return None

def get_last_loaded_date():
    query = "SELECT MAX(data_loaded_at) FROM fire_incidents;"
    with psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            last_loaded_date = cursor.fetchone()[0]
            return last_loaded_date or (datetime.now() - timedelta(days=1))

def clear_staging_table():
    with psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM staging_fire_incidents;")
        conn.commit()
    print("staging_fire_incidents table cleared.")

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
                insert_query = f"INSERT INTO staging_fire_incidents ({columns_sql}) VALUES ({values_sql})"

                for _, row in df.iterrows():
                    values = [str(row[col]) if pd.notnull(row[col]) else None for col in df.columns]
                    cursor.execute(insert_query, values)

                conn.commit()
        print("Batch data loaded into the staging_fire_incidents table.")
    except psycopg2.DatabaseError as e:
        print(f"Database error during data insertion: {e}")
    except Exception as e:
        print(f"General error during data insertion: {e}")

def fetch_and_load_data():
    clear_staging_table()
    last_loaded_date = get_last_loaded_date()
    offset = 0

    while True:
        last_loaded_date_str = last_loaded_date.strftime("%Y-%m-%dT%H:%M:%S")
        response = requests.get(f"{API_URL}?$limit={LIMIT}&$offset={offset}&$where=data_loaded_at > '{last_loaded_date_str}'")
        response.raise_for_status()
        data = response.json()

        if not data:
            print("No new data to process.")
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

if __name__ == "__main__":
    fetch_and_load_data()
