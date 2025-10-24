import os
import time
import pandas as pd
import psycopg2
import openmeteo_requests
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone

sb_user = os.getenv('SUPABASE_USER')
sb_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{sb_user}:{sb_password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

turkey_timezone = timezone(timedelta(hours=3))

cities = {
    "istanbul": {"latitude": 41.0138, "longitude": 28.9497},
    "izmir": {"latitude": 38.4183, "longitude": 27.1951},
    "adana": {"latitude": 37.0123, "longitude": 35.3826},
    "ankara": {"latitude": 39.8946, "longitude": 32.7615},
    "samsun": {"latitude": 41.3005, "longitude": 36.2841},
    "erzurum": {"latitude": 39.8946, "longitude": 41.1716},
    "gaziantep": {"latitude": 37.0826, "longitude": 37.4505}
}

openmeteo = openmeteo_requests.Client()

url = "https://api.open-meteo.com/v1/forecast"
common_params = {
    "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", 
                "weather_code", "cloud_cover", "wind_speed_10m"],
    "forecast_days": 16
}

city_dataframes = {}

for city, coords in cities.items():
    params = {**common_params, **coords}
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "precipitation": hourly.Variables(2).ValuesAsNumpy(),
        "weather_code": hourly.Variables(3).ValuesAsNumpy(),
        "cloud_cover": hourly.Variables(4).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(5).ValuesAsNumpy()
    }

    df = pd.DataFrame(hourly_data)
    df.set_index("date", inplace=True)
    df = df.applymap(lambda x: float(f"{x:.1f}"))
    city_dataframes[city] = df

    time.sleep(0.3)

temperature_df = pd.concat([df["temperature_2m"].rename(city) for city, df in city_dataframes.items()], axis=1)
humidity_df = pd.concat([df["relative_humidity_2m"].rename(city) for city, df in city_dataframes.items()], axis=1)
precipitation_df = pd.concat([df["precipitation"].rename(city) for city, df in city_dataframes.items()], axis=1)
weather_code_df = pd.concat([df["weather_code"].rename(city) for city, df in city_dataframes.items()], axis=1)
cloud_cover_df = pd.concat([df["cloud_cover"].rename(city) for city, df in city_dataframes.items()], axis=1)
wind_speed_df = pd.concat([df["wind_speed_10m"].rename(city) for city, df in city_dataframes.items()], axis=1)

weather_codes = pd.read_json('weather_codes.json')
code_to_desc = {int(k): v["day"]["description"] for k, v in weather_codes.items()}
for city in weather_code_df.columns:
    if city != "date":
        weather_code_df[city] = weather_code_df[city].map(code_to_desc)

tables = {
    "temperature": temperature_df,
    "humidity": humidity_df,
    "precipitation": precipitation_df,
    "weather_condition": weather_code_df,
    "cloud_cover": cloud_cover_df,
    "wind_speed": wind_speed_df
}

for df in tables.values():
    df.index = df.index.tz_localize(None)
    df.index = df.index.astype(str)

with engine.begin() as conn:

    for table_name, df in tables.items():
        
        timestamps = df.index.unique().tolist()
        conn.execute(
            text(f"""
                 DELETE FROM openmeteo.{table_name}
                 WHERE date IN :timestamps
                 """),
                 {"timestamps": tuple(timestamps)}
            )
        
        df.to_sql(table_name, conn, if_exists='append', index=True, schema='openmeteo', method='multi')
        print(f"{table_name} was uploaded!")

print(f"All data was uploaded to DB at {datetime.now(turkey_timezone).isoformat()}!")
print("Succeed!")