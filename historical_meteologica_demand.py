import os
import requests
import pandas as pd
import psycopg2
import shutil
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
from loguru import logger

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
selected_ref_date = datetime.now(turkey_timezone) - timedelta(days=2)
selected_ref_end_date = selected_ref_date + timedelta(days=4)
ref_year = selected_ref_date.year
ref_month = selected_ref_date.month
ref_day = selected_ref_date.day
logger.info(f"Target reference date: {selected_ref_date.year}-{selected_ref_date.month:02}-{selected_ref_date.day:02}")

#-----------------------------------------------------------------------------------------------------------

data_dict = {'1456': 'ens', '7319': 'ensext'}

pred_data = []

for data_code, data in zip(data_dict.keys(), data_dict.values()):

    if selected_ref_date.day >= 2:

        response = requests.get(
            url=f"https://api-markets.meteologica.com/api/v1/contents/{data_code}/historical_data/{ref_year}/{ref_month}",
            params={"token": response_data.get("token")})

        logger.info(f"{ref_year}-{ref_month} {data} data extracted...")

    else:

        response = requests.get(
            url=f"https://api-markets.meteologica.com/api/v1/contents/{data_code}/historical_data/{ref_year}/{ref_month-1}",
            params={"token": response_data.get("token")})

        logger.info(f"{ref_year}-{ref_month} {data} data extracted...")

    zip_filename = f'historical_data_demand.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_demand"
    shutil.unpack_archive(zip_filename, extract_dir)
    logger.debug(f"{zip_filename} exported to: {extract_dir}")

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        logger.debug(f"Temporary files for {zip_filename} removed.")

    if data == 'ens':
        forecast_hour = 12
    elif data == 'ensext':
        forecast_hour = 0

    if selected_ref_date.day == 1:
        data_files = [file for file in os.listdir(extract_dir) if ('post' not in file) & (file.startswith(f'{data_code}_{ref_year}{ref_month-1:02}{(selected_ref_date - timedelta(days=1)).day:02}{forecast_hour:02}00.json'))]

    else:
        data_files = [file for file in os.listdir(extract_dir) if ('post' not in file) & (file.startswith(f'{data_code}_{ref_year}{ref_month:02}{(selected_ref_date - timedelta(days=1)).day:02}{forecast_hour:02}00.json'))]

    df = pd.read_json(extract_dir + '/' + data_files[0])
    final_data = pd.DataFrame(list(df['data'].values))
    final_data = final_data[['From yyyy-mm-dd hh:mm', 'Bottom', 'Average', 'Top']].copy()
    final_data.columns = ['date',f'forecast_{data_dict[data_code]}_low', f'forecast_{data_dict[data_code]}_avg', f'forecast_{data_dict[data_code]}_high']
    final_data['date'] = pd.to_datetime(final_data['date'])
    pred_data.append(final_data[(final_data['date'].dt.date >= selected_ref_date.date()) &
                                (final_data['date'].dt.date <= selected_ref_end_date.date())])
    logger.success(f"{data} files opened and extracted successfully...")

processed_data = [df.set_index('date') for df in pred_data]
ref_df = pd.concat(processed_data, axis=1)

#-----------------------------------------------------------------------------------------------------------

db_user = os.getenv('SUPABASE_USER')
db_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{db_user}:{db_password}@aws-0-us-east-2.pooler.supabase.com:6543/postgres"
engine = create_engine(connection_str)

try:
    with engine.begin() as conn:

        for loc, day in zip(range(0, 121, 24), range(1,6)):

            ref_df.iloc[loc:loc+24].to_sql(f'historical_demand_d+{day}', conn, if_exists='append', index=True, schema='meteologica', method='multi')
            
        logger.success(f"Historical forecasts for {selected_ref_date.year}-{selected_ref_date.month:02}-{selected_ref_date.day:02} uploaded to the database!")

except Exception as e:
        logger.critical(f"Database upload failed: {e}")
