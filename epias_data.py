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

turkey_timezone = timezone(timedelta(hours=3))

today_start = datetime.now(turkey_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
month_start = today_start.replace(day=1)
tomorrow_start = today_start + timedelta(days=1)

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/dam/data/mcp"

if datetime.now(turkey_timezone).hour < 14:
    response_url = requests.post(
        service_url,
        json={"startDate": str(month_start.isoformat()),
            "endDate": str(today_start.isoformat())},
        headers={"Accept-Language":"en",
                "Accept":"application/json",
                "Content-Type":"application/json",
                "TGT":tgt_code},
        timeout=30
    )

else:
    response_url = requests.post(
        service_url,
        json={"startDate": str(month_start.isoformat()),
            "endDate": str(tomorrow_start.isoformat())},
        headers={"Accept-Language":"en",
                "Accept":"application/json",
                "Content-Type":"application/json",
                "TGT":tgt_code},
        timeout=30
    )

if response_url.status_code == 200:
    response = response_url.json()

else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

ptf_df = pd.DataFrame.from_records(response['items'])

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/bpm/data/system-marginal-price"

response_url = requests.post(
    service_url,
    json={"startDate": str(month_start.isoformat()),
        "endDate": str(today_start.isoformat())},
    headers={"Accept-Language":"en",
            "Accept":"application/json",
            "Content-Type":"application/json",
            "TGT":tgt_code},
    timeout=30
    )

if response_url.status_code == 200:
    response = response_url.json()

else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

smf_df = pd.DataFrame.from_records(response['items']).drop(columns='hour')

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/bpm/data/order-summary-up"

response_url = requests.post(
    service_url,
    json={"startDate": str((month_start - pd.Timedelta(days=1)).isoformat()),
        "endDate": str((today_start - pd.Timedelta(days=1)).isoformat())},
    headers={"Accept-Language":"en",
            "Accept":"application/json",
            "Content-Type":"application/json",
            "TGT":tgt_code},
    timeout=30
    )

if response_url.status_code == 200:
    response = response_url.json()

else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

yal_df = pd.DataFrame.from_records(response['items'])
yal_df.columns = ['date', 'hour', 'yal0', 'yal1', 'yal2', 'yal_delivered', 'net']
yal_df['yal_total'] = yal_df['yal0'] + yal_df['yal1'] + yal_df['yal2']

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/generation/data/realtime-generation"

response_url = requests.post(
    service_url,
    json={"startDate": str(month_start.isoformat()),
        "endDate": str(tomorrow_start.isoformat())},
    headers={"Accept-Language":"en",
            "Accept":"application/json",
            "Content-Type":"application/json",
            "TGT":tgt_code},
    timeout=30
            )
    
if response_url.status_code == 200:
    response = response_url.json()
    
else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

realtime_generation_df = pd.DataFrame.from_records(response['items'])

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/generation/data/dpp"

if datetime.now(turkey_timezone).hour < 14:
    response_url = requests.post(
        service_url,
        json={"startDate": str(month_start.isoformat()),
            "endDate": str(today_start.isoformat()),
            "region":"TR1"},
        headers={"Accept-Language":"en",
                "Accept":"application/json",
                "Content-Type":"application/json",
                "TGT":tgt_code},
        timeout=30
    )

else:
    response_url = requests.post(
        service_url,
        json={"startDate": str(month_start.isoformat()),
            "endDate": str(tomorrow_start.isoformat()),
            "region":"TR1"},
        headers={"Accept-Language":"en",
                "Accept":"application/json",
                "Content-Type":"application/json",
                "TGT":tgt_code},
        timeout=30
    )

if response_url.status_code == 200:
    response = response_url.json()

else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

kgüp_df = pd.DataFrame.from_records(response['items'])

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/generation/data/dpp-first-version"

if datetime.now(turkey_timezone).hour < 14:
    response_url = requests.post(
        service_url,
        json={"startDate": str(month_start.isoformat()),
            "endDate": str(today_start.isoformat()),
            "region":"TR1"},
        headers={"Accept-Language":"en",
                "Accept":"application/json",
                "Content-Type":"application/json",
                "TGT":tgt_code},
        timeout=30
    )

else:
    response_url = requests.post(
        service_url,
        json={"startDate": str(month_start.isoformat()),
            "endDate": str(tomorrow_start.isoformat()),
            "region":"TR1"},
        headers={"Accept-Language":"en",
                "Accept":"application/json",
                "Content-Type":"application/json",
                "TGT":tgt_code},
        timeout=30
    )

if response_url.status_code == 200:
    response = response_url.json()

else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

kgüp_v1_df = pd.DataFrame.from_records(response['items'])

#---------------------------------------------------------------------------------------------------------------------------------
#str(month_start.isoformat())
service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/data/market-message-system"

response_url = requests.post(
    service_url,
    json={"startDate": str(month_start.isoformat()), 
        "endDate": str((datetime.now(turkey_timezone)).isoformat()),
        "regionId": 1},
    headers={"Accept-Language":"en",
            "Accept":"application/json",
            "Content-Type":"application/json",
            "TGT":tgt_code},
    timeout=30
            )
    
if response_url.status_code == 200:
    response = response_url.json()
    
else:
    print(f"Hata: {response_url.status_code}, Mesaj: {response_url.text}")

message_df = pd.DataFrame.from_records(response['items'])
message_df = message_df.iloc[:, :7].drop(columns='powerPlantName').copy()
message_df.drop_duplicates(inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

sb_user = os.getenv('SUPABASE_USER')
sb_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{sb_user}:{sb_password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

tables = {
    "ptf": ptf_df,
    "smf": smf_df,
    "yal": yal_df,
    "realtime_generation": realtime_generation_df,
    "kgüp_v1": kgüp_v1_df,
    "kgüp": kgüp_df,
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
