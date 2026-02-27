import requests
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone

username = os.getenv('XTRADERS_USERNAME')
password = os.getenv('XTRADERS_PASSWORD')

sb_user = os.getenv('SUPABASE_USER')
sb_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{sb_user}:{sb_password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

url = "https://api-markets.meteologica.com/api/v1/login"

data = {
    "user": username,
    "password": password
}

response = requests.post(url, json=data)

if response.status_code == 200:
    response_data = response.json()
    print("Token:", response_data.get("token"))
    print("Token Son Kullanım Tarihi:", response_data.get("expiration_date"))
    
else:
    print(f"Hata: {response.status_code}, Mesaj: {response.text}")

turkey_timezone = timezone(timedelta(hours=3))
current_year = datetime.now().year
current_month = datetime.now().month
print(f"Year: {current_year}, Month: {current_month}")

#---------------------------------------------------------------------------------------------------------------------------------

# Forecast
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/4688/data',
  params = {"token": response_data.get("token")}
)

forecast_updated_data = response.json()['data']
forecast_updated_data = pd.DataFrame(forecast_updated_data)[['From yyyy-mm-dd hh:mm', 'Bottom', 'Average', 'Top']]
forecast_updated_data.columns = ['date', 'min_price', 'avg_price', 'max_price']
forecast_updated_data['date'] = pd.to_datetime(forecast_updated_data['date'])

if datetime.now(turkey_timezone).hour >= 8 and datetime.now(turkey_timezone).hour < 12:
    forecast_updated_data = forecast_updated_data[forecast_updated_data["date"].dt.date == (datetime.now(turkey_timezone).date() + timedelta(days=1))]

    try:

        with engine.connect() as conn:

            forecast_updated_data.to_sql('meteologica_forecast', conn, if_exists='append', index=False, schema='public', method='multi')
            print(f"price data was uploaded at {datetime.now(turkey_timezone).isoformat()}!")

    except:

        print(f"The D+1 forecasts has already loaded into database!")

elif datetime.now(turkey_timezone).hour >= 13 and datetime.now(turkey_timezone).hour < 15:
    forecast_updated_data = forecast_updated_data[forecast_updated_data["date"].dt.date == (datetime.now(turkey_timezone).date() + timedelta(days=2))]

    try:

        with engine.connect() as conn:

            forecast_updated_data.to_sql('meteologica_forecast_d+2', conn, if_exists='append', index=False, schema='public', method='multi')
            print(f"price data was uploaded at {datetime.now(turkey_timezone).isoformat()}!")

    except:

        print(f"The D+2 forecasts has already loaded into database!")
        
#---------------------------------------------------------------------------------------------------------------------------------

# Price
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/4687/data',
  params = {"token": response_data.get("token")}
)

price_updated_data = pd.DataFrame(response.json()['data'])
price_updated_data.drop(columns=['UTC offset from (UTC+/-hhmm)', 'UTC offset to (UTC+/-hhmm)'], inplace=True)
price_updated_data.columns = ['From-yyyy-mm-dd-hh-mm', 'To-yyyy-mm-dd-hh-mm', 'price_forecast']

#---------------------------------------------------------------------------------------------------------------------------------

# Unlicensed Solar
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/1430/data',
  params = {"token": response_data.get("token")}
)

unlicensed_solar_updated_data = pd.DataFrame(response.json()['data'])
unlicensed_solar_updated_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN',
                                            'UTC offset from (UTC+/-hhmm)', 'UTC offset to (UTC+/-hhmm)'], inplace=True, errors='ignore')
unlicensed_solar_updated_data.rename(columns={col: f"unlicensed_{col}" for col in unlicensed_solar_updated_data.columns[2:]}, inplace=True)
for column in unlicensed_solar_updated_data.columns[2:]:
    unlicensed_solar_updated_data[column] = unlicensed_solar_updated_data[column].astype(int)

#---------------------------------------------------------------------------------------------------------------------------------

# Licensed Solar
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/1429/data',
  params = {"token": response_data.get("token")}
)

licensed_solar_updated_data = pd.DataFrame(response.json()['data'])
licensed_solar_updated_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN',
                                            'UTC offset from (UTC+/-hhmm)', 'UTC offset to (UTC+/-hhmm)'], inplace=True, errors='ignore')
licensed_solar_updated_data.rename(columns={col: f"licensed_{col}" for col in licensed_solar_updated_data.columns[2:]}, inplace=True)
for column in licensed_solar_updated_data.columns[2:]:
    licensed_solar_updated_data[column] = licensed_solar_updated_data[column].astype(int)

#---------------------------------------------------------------------------------------------------------------------------------

# Wind
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/1446/data',
  params = {"token": response_data.get("token")}
)

wind_updated_data = pd.DataFrame(response.json()['data'])
wind_updated_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN',
                                'UTC offset from (UTC+/-hhmm)', 'UTC offset to (UTC+/-hhmm)'], inplace=True, errors='ignore')
wind_updated_data.rename(columns={col: f"wind_{col}" for col in wind_updated_data.columns[2:]}, inplace=True)
for column in wind_updated_data.columns[2:]:
    wind_updated_data[column] = wind_updated_data[column].astype(int)

#---------------------------------------------------------------------------------------------------------------------------------

# DAM
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/1459/data',
  params = {"token": response_data.get("token")}
)

dam_updated_data = pd.DataFrame(response.json()['data'])
dam_updated_data.drop(columns=['UTC offset from (UTC+/-hhmm)', 'UTC offset to (UTC+/-hhmm)'], inplace=True, errors='ignore')
dam_updated_data.rename(columns={'forecast': 'conventional_forecast'}, inplace=True)
for column in dam_updated_data.columns[2:]:
    dam_updated_data[column] = dam_updated_data[column].astype(int)

#---------------------------------------------------------------------------------------------------------------------------------

# ROR
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/1463/data',
  params = {"token": response_data.get("token")}
)

ror_updated_data = pd.DataFrame(response.json()['data'])
ror_updated_data.drop(columns=['UTC offset from (UTC+/-hhmm)', 'UTC offset to (UTC+/-hhmm)'], inplace=True, errors='ignore')
ror_updated_data.rename(columns={'forecast': 'runofriver_forecast'}, inplace=True)
for column in ror_updated_data.columns[2:]:
    ror_updated_data[column] = ror_updated_data[column].astype(int)

#---------------------------------------------------------------------------------------------------------------------------------

# Demand
response = requests.get(
  url = 'https://api-markets.meteologica.com/api/v1/contents/1455/data',
  params = {"token": response_data.get("token")}
)

demand_updated_data = pd.DataFrame(response.json()['data'])
demand_updated_data.drop(columns=['UTC offset from (UTC+/-hhmm)', 'UTC offset to (UTC+/-hhmm)'], inplace=True, errors='ignore')
demand_updated_data.rename(columns={'forecast': 'demand_forecast'}, inplace=True)
for column in demand_updated_data.columns[2:]:
    demand_updated_data[column] = demand_updated_data[column].astype(int)

#---------------------------------------------------------------------------------------------------------------------------------

# Database Upload
tables = {
    "price": price_updated_data,
    "unlicensed_solar": unlicensed_solar_updated_data,
    "licensed_solar": licensed_solar_updated_data,
    "wind": wind_updated_data,
    "dam_hydro": dam_updated_data,
    "runofriver_hydro": ror_updated_data,
    "demand": demand_updated_data
}

with engine.begin() as conn:
    for table_name, df in tables.items():
        df.columns = df.columns.str.replace(' ','-').str.replace(':','-')
        timestamps = df["From-yyyy-mm-dd-hh-mm"].unique().tolist()

        conn.execute(
            text(f"""
                DELETE FROM meteologica.{table_name}
                WHERE "From-yyyy-mm-dd-hh-mm" IN :timestamps
            """),
            {"timestamps": tuple(timestamps)}
        )

        df.to_sql(table_name, conn, if_exists='append', index=False, schema='meteologica', method='multi')
        print(f"{table_name} was uploaded at {datetime.now(turkey_timezone).isoformat()}")

print("Succeed!")
