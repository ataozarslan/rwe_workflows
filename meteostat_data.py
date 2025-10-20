import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
from meteostat import Point, Hourly

sb_user = os.getenv('SUPABASE_USER')
sb_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{sb_user}:{sb_password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

# Türkiye saat dilimi (UTC+3)
turkey_timezone = timezone(timedelta(hours=3))

now = datetime.now(turkey_timezone)
next_week = now + timedelta(weeks=1)

cities = {
    "istanbul": Point(41.01, 28.97),
    "izmir": Point(38.42, 27.14),
    "adana": Point(37.00, 35.32),
    "ankara": Point(39.93, 32.85),
    "samsun": Point(41.29, 36.33),
    "erzurum": Point(39.90, 41.27),
    "gaziantep": Point(37.07, 37.38)
}

weights = {
    "istanbul": 0.302,   # Marmara
    "izmir": 0.128,      # Ege
    "adana": 0.106,      # Akdeniz
    "ankara": 0.130,     # İç Anadolu
    "samsun": 0.095,     # Karadeniz
    "erzurum": 0.080,    # Doğu Anadolu
    "gaziantep": 0.059   # Güneydoğu
}

start = datetime(now.year, now.month, now.day)
end = datetime(next_week.year, next_week.month, next_week.day, next_week.hour+1)

data = pd.DataFrame()

for name, loc in cities.items():
    df_city = Hourly(loc, start, end).fetch()[['temp']].copy()
    df_city.columns = [name]
    df_city[name] = df_city[name].astype(float)
    
    if data.empty:
        data = df_city
    else:
        data = data.merge(df_city, how='outer', left_index=True, right_index=True)

data = data.interpolate(method='linear', limit_direction='both')

weighted_temp = pd.Series(0, index=data.index)
for city in cities.keys():
    weighted_temp += data[city] * weights[city]

data['weighted_temp'] = weighted_temp.round(2)

data.reset_index(inplace=True)
data.rename(columns={'time': 'date'}, inplace=True)
data['date'] = data['date'].astype(str)

with engine.begin() as conn:
        
    timestamps = data["date"].unique().tolist()

    conn.execute(
        text(f"""
             DELETE FROM public.hourly_temperature
             WHERE date IN :timestamps
            """),
            {"timestamps": tuple(timestamps)}
        )

    data.to_sql('hourly_temperature', conn, if_exists='append', index=False, schema='public', method='multi')

print(f"All data was uploaded to DB at {datetime.now(turkey_timezone).isoformat()}!")
print("Succeed!")
