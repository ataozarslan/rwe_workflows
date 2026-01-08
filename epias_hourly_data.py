import time
import requests
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

username = os.getenv('EPIAS_USERNAME')
password = os.getenv('EPIAS_PASSWORD')

url = "https://giris.epias.com.tr/cas/v1/tickets"

data = {
    "username":username,
    "password":password}

response_tgt = requests.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded","Accept": "text/plain"}, timeout=30)

if response_tgt.status_code == 201:
    tgt_code = response_tgt.text
    print("TGT:", tgt_code)
else:
    print(f"Hata: {response_tgt.status_code}, Mesaj: {response_tgt.text}")

def safe_post(url, json=None, headers=None, retries=5, timeout=60):
    for i in range(retries):
        try:
            response = requests.post(url, json=json, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response
            else:
                print(f"[{i+1}/{retries}] HTTP {response.status_code} error, retrying...")
        except requests.exceptions.ReadTimeout:
            print(f"[{i+1}/{retries}] ReadTimeout error, retrying...")
        except requests.exceptions.RequestException as e:
            print(f"[{i+1}/{retries}] Other error: {e}, retrying...")
        time.sleep(5)
    raise Exception(f"Request to {url} failed after {retries} retries.")

turkey_timezone = timezone(timedelta(hours=3))

today_start = datetime.now(turkey_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
last_week_start = today_start - timedelta(days=7)
tomorrow_start = today_start + timedelta(days=1)

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/generation/data/realtime-generation"

response_url = safe_post(
    service_url,
    json={"startDate": str(last_week_start.isoformat()),
        "endDate": str(tomorrow_start.isoformat())},
    headers={"Accept-Language":"en",
            "Accept":"application/json",
            "Content-Type":"application/json",
            "TGT":tgt_code}
)
    
if response_url.status_code == 200:
    response = response_url.json()
    
else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

realtime_generation_df = pd.DataFrame.from_records(response['items'])

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/consumption/data/realtime-consumption"

response_url = safe_post(
        service_url,
        json={"startDate": str(last_week_start.isoformat()),
            "endDate": str(today_start.isoformat())},
        headers={"Accept-Language":"en",
                "Accept":"application/json",
                "Content-Type":"application/json",
                "TGT":tgt_code}
)
    
if response_url.status_code == 200:
    response = response_url.json()
    
else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

consumption_df = pd.DataFrame.from_records(response['items'])

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/data/market-message-system"

response_url = safe_post(
    service_url,
    json={"startDate": str(last_week_start.isoformat()), 
        "endDate": str((datetime.now(turkey_timezone)).isoformat()),
        "regionId": 1},
    headers={"Accept-Language":"en",
            "Accept":"application/json",
            "Content-Type":"application/json",
            "TGT":tgt_code}
)
    
if response_url.status_code == 200:
    response = response_url.json()
    
else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

message_df = pd.DataFrame.from_records(response['items'])
message_df = message_df.iloc[:, :7].drop(columns='powerPlantName').copy()
message_df['caseStartDate'] = pd.to_datetime(message_df['caseStartDate']).dt.tz_localize(None)
message_df['caseEndDate'] = pd.to_datetime(message_df['caseEndDate']).dt.tz_localize(None)
message_df.drop_duplicates(inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

sb_user = os.getenv('SUPABASE_USER')
sb_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{sb_user}:{sb_password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

tables = {
    "realtime_generation": realtime_generation_df,
    "realtime_consumption": consumption_df
    "market_messages": message_df
}

with engine.begin() as conn:
    for table_name, df in tables.items():
        
        if "date" in df.columns:
            timestamps = df["date"].unique().tolist()
            conn.execute(
                text(f"""
                    DELETE FROM epias.{table_name}
                    WHERE date IN :timestamps
                """),
                {"timestamps": tuple(timestamps)}
            )
            df.to_sql(table_name, conn, if_exists='append', index=False, schema='epias', method='multi')
            print(f"{table_name} was uploaded!")

        else:
            db_df = pd.read_sql_table(table_name, conn, schema='epias')

            merged = df.merge(db_df.drop_duplicates(), how='left', indicator=True)
            
            new_rows = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

            if not new_rows.empty:
                new_rows.to_sql(table_name, conn, if_exists='append', index=False, schema='epias', method='multi')
                print(f"{table_name} was uploaded!")

print(f"All data was uploaded to DB at {datetime.now(turkey_timezone).isoformat()}!")
print("Succeed!")