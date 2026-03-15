import os
import requests
import pandas as pd
import psycopg2
import shutil
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
from loguru import logger

epias_username = os.getenv('EPIAS_USERNAME')
epias_password = os.getenv('EPIAS_PASSWORD')

db_user = os.getenv('SUPABASE_USER')
db_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{db_user}:{db_password}@aws-0-us-east-2.pooler.supabase.com:6543/postgres"
engine = create_engine(connection_str)

meteo_username = os.getenv('XTRADERS_USERNAME')
meteo_password = os.getenv('XTRADERS_PASSWORD')

url = "https://api-markets.meteologica.com/api/v1/login"

data = {
    "user": meteo_username,
    "password": meteo_password
}

response = requests.post(url, json=data)

if response.status_code == 200:
    response_data = response.json()
    logger.success(f"Login successful. Token expires at: {response_data.get('expiration_date')}")
else:
    logger.error(f"Login failed! Status: {response.status_code}, Response: {response.text}")
    exit()

turkey_timezone = timezone(timedelta(hours=3))
today_date = datetime.now(turkey_timezone)
selected_ref_date = datetime.now(turkey_timezone) - timedelta(days=1)
ref_year = selected_ref_date.year
ref_month = selected_ref_date.month
ref_day = selected_ref_date.day
logger.info(f"Target reference date: {ref_year}-{ref_month:02}-{ref_day:02}")

#-----------------------------------------------------------------------------------------------------------

pred_data = []
endpoints = {'demand': 1455,
             'unlicensed_solar': 1430,
             'licensed_solar': 1429,
             'wind': 1446,
             'runofriver': 1463}

for data, data_code in zip(endpoints.keys(), endpoints.values()):

    if selected_ref_date.day >= 3:

        response = requests.get(
            url=f"https://api-markets.meteologica.com/api/v1/contents/{data_code}/historical_data/{ref_year}/{ref_month}",
            params={"token": response_data.get("token")})
        logger.info(f"{ref_year}-{ref_month} {data} data extracted...")
        
    else:

        response = requests.get(
            url=f"https://api-markets.meteologica.com/api/v1/contents/{data_code}/historical_data/{ref_year}/{ref_month-1}",
            params={"token": response_data.get("token")})
        logger.info(f"{ref_year}-{ref_month-1} {data} data extracted...")

    zip_filename = f'historical_data_{data}.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_{data}"
    shutil.unpack_archive(zip_filename, extract_dir)
    logger.debug(f"{zip_filename} exported to: {extract_dir}")

    if selected_ref_date.day == 2:
        data_files = [file for file in os.listdir(f'historical_data_{data}') if ('post' not in file) & (file.startswith(f'{data_code}_{ref_year}{ref_month-1:02}{(selected_ref_date - timedelta(days=2)).day:02}11'))]

    elif selected_ref_date.day == 1:
        data_files = [file for file in os.listdir(f'historical_data_{data}') if ('post' not in file) & (file.startswith(f'{data_code}_{ref_year}{ref_month-1:02}{(selected_ref_date - timedelta(days=1)).day:02}11'))]

    else:
        data_files = [file for file in os.listdir(f'historical_data_{data}') if ('post' not in file) & (file.startswith(f'{data_code}_{ref_year}{ref_month:02}{(selected_ref_date - timedelta(days=1)).day:02}11'))]

    df = pd.read_json(f'historical_data_{data}/{data_files[0]}')
    final_data = pd.DataFrame(list(df['data'].values))
    final_data = final_data[['From yyyy-mm-dd hh:mm','forecast']].copy()
    final_data.columns = ['date',f'{data}_forecast']
    final_data['date'] = pd.to_datetime(final_data['date'])

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        logger.debug(f"Temporary files for {zip_filename} removed.")

    pred_data.append(final_data[(final_data['date'].dt.date == selected_ref_date.date()) | 
                                (final_data['date'].dt.date == today_date.date())])

processed_data = [df.set_index('date') for df in pred_data]
ref_df = pd.concat(processed_data, axis=1)

#-----------------------------------------------------------------------------------------------------------

try:
    with engine.begin() as conn:

        timestamps = ref_df.index.unique().tolist()

        conn.execute(
            text(f"""
                DELETE FROM meteologica.historical_forecast
                WHERE date IN :timestamps
                """),
            {"timestamps": tuple(timestamps)}
        )

        ref_df.to_sql('historical_forecast', conn, if_exists='append', index=True, schema='meteologica', method='multi')
        logger.success(f"Historical forecasts for {ref_year}-{ref_month:02}-{ref_day:02} uploaded to the database!")

except Exception as e:
        logger.critical(f"Database upload failed: {e}")
