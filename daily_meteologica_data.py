import os
import requests
import pandas as pd
import psycopg2
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
selected_ref_date = datetime.now(turkey_timezone)
tomorrow_date = datetime.now(turkey_timezone) + timedelta(days=1)
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

update_ids = {'demand': f'{ref_year}{ref_month:02}{ref_day:02}1115',
             'unlicensed_solar': f'{ref_year}{ref_month:02}{ref_day:02}1113',
             'licensed_solar': f'{ref_year}{ref_month:02}{ref_day:02}1113',
             'wind': f'{ref_year}{ref_month:02}{ref_day:02}1114',
             'runofriver': f'{ref_year}{ref_month:02}{ref_day:02}1150'}

for data, data_code in zip(endpoints.keys(), endpoints.values()):

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/{data_code}/data",
        params={"token": response_data.get("token"),
                "update_id": update_ids[data]})
    
    logger.info(f"{ref_year}-{ref_month}-{ref_day} {data} data extracted...")

    final_data = pd.DataFrame(response.json()['data'])
    final_data = final_data[['From yyyy-mm-dd hh:mm','forecast']].copy()
    final_data.columns = ['date',f'{data}_forecast']
    final_data['date'] = pd.to_datetime(final_data['date'])

    pred_data.append(final_data[(final_data['date'].dt.date == selected_ref_date.date()) | 
                                (final_data['date'].dt.date == tomorrow_date.date())])

processed_data = [df.set_index('date') for df in pred_data]
ref_df = pd.concat(processed_data, axis=1)
ref_df = ref_df.iloc[1:].copy()

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
