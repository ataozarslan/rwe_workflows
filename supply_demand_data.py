import requests
import os
import pandas as pd
import psycopg2
import time
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, timezone
from requests.exceptions import ChunkedEncodingError, Timeout, ConnectionError

username = os.getenv('EPIAS_USERNAME')
password = os.getenv('EPIAS_PASSWORD')

#---------------------------------------------------------------------------------------------------------------------------------

url = "https://giris.epias.com.tr/cas/v1/tickets"

data = {
    "username":username,
    "password":password}

response_tgt = requests.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded","Accept": "text/plain"}, timeout=30)

# TGT yanıtını kontrol et
if response_tgt.status_code == 201:
    tgt_code = response_tgt.text
    print("TGT:", tgt_code)
else:
    print(f"Hata: {response_tgt.status_code}, Mesaj: {response_tgt.text}")

#---------------------------------------------------------------------------------------------------------------------------------

turkey_timezone = timezone(timedelta(hours=3))

today_start = datetime.now(turkey_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
month_start = today_start.replace(day=1)
tomorrow_start = today_start + timedelta(days=1)

print("Ayın başlangıcı:", month_start.isoformat())
print("Bugünün başlangıcı:", today_start.isoformat())
print("Yarın başlangıcı:", tomorrow_start.isoformat())

#---------------------------------------------------------------------------------------------------------------------------------

service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/dam/data/supply-demand"

if datetime.now(turkey_timezone).hour >= 14:
    end_date = datetime.now(turkey_timezone) + timedelta(days=1)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
else:
    end_date = datetime.now(turkey_timezone)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

day_start = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
day_end = end_date.replace(hour=23, minute=0, second=0, microsecond=0)

all_data = []
current = day_start

while current <= day_end:
    date_str = current.strftime("%Y-%m-%dT%H:00:00+03:00")
    retry_count = 0

    while retry_count < 5:
        try:
            response = requests.post(
                service_url,
                json={"date": date_str},
                headers={
                    "Accept-Language": "en",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "TGT": tgt_code
                },
                timeout=15
            )

            if response.status_code == 200:
                json_data = response.json()
                all_data.extend(json_data.get("items", []))
                print(f"✓ {date_str}")
                break

            elif response.status_code == 429:
                wait_time = 5 * (retry_count + 1)
                print(f"⏳ 429 Too Many Requests - {date_str} - {wait_time}s bekleniyor...")
                time.sleep(wait_time)
                retry_count += 1

            else:
                print(f"⚠️ Hata: {response.status_code} - {date_str}")
                break

        except (ChunkedEncodingError, Timeout, ConnectionError) as e:
            wait_time = 5 * (retry_count + 1)
            print(f"🔁 Ağ Hatası ({type(e).__name__}) - {date_str}: {str(e)} - {wait_time}s bekleniyor...")
            time.sleep(wait_time)
            retry_count += 1

        except Exception as e:
            print(f"🚨 Bilinmeyen Hata - {date_str}: {str(e)}")
            break

    time.sleep(1)
    current += timedelta(hours=1)

supply_demand_df = pd.DataFrame(all_data)

#---------------------------------------------------------------------------------------------------------------------------------

sb_user = os.getenv('SUPABASE_USER')
sb_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{sb_user}:{sb_password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

target_date = (datetime.now(turkey_timezone) - timedelta(days=14)).date().isoformat()

with engine.begin() as conn:

    conn.execute(text("""
        DELETE FROM epias.supply_demand
        WHERE DATE(date) = :target_date;
    """), {"target_date": target_date})
    
    supply_demand_df.to_sql('supply_demand', conn, if_exists='append', index=False, schema='epias', method='multi')

print(f"All data was uploaded to DB at {datetime.now(turkey_timezone).isoformat()}!")
print("Succeed!")
