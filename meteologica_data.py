import requests
import os
import pandas as pd
import shutil
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone

username = os.getenv('XTRADERS_USERNAME')
password = os.getenv('XTRADERS_PASSWORD')

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

current_year = datetime.now().year
current_month = datetime.now().month
print(f"Year: {current_year}, Month: {current_month}")

turkey_timezone = timezone(timedelta(hours=3))

#---------------------------------------------------------------------------------------------------------------------------------

try:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1430/historical_data/{current_year}/{current_month}", # 1444
        params={"token": response_data.get("token")})

    zip_filename = 'historical_data_solarpv_unlicensed.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_solarpv_unlicensed"
    shutil.unpack_archive(zip_filename, extract_dir)
    print(f"{zip_filename} klasöre çıkarıldı: {extract_dir}")

    unlicensed_solarpv_data_files = [file for file in os.listdir('historical_data_solarpv_unlicensed') if 'post' not in file]

    data = []
    other_data = []

    for file in unlicensed_solarpv_data_files:
        df = pd.read_json(f'historical_data_solarpv_unlicensed/{file}')
        data.append(pd.DataFrame(list(df['data'].values)))
        other_data.append(df.drop(columns='data'))

    data = pd.concat(data, ignore_index=True)
    other_data = pd.concat(other_data, ignore_index=True)
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN', 'UTC offset from (UTC+/-hhmm)',
                            'UTC offset to (UTC+/-hhmm)', 'content_name', 'content_id', 'timezone', 'unit'], inplace=True)

except:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1430/data", # 1444
        params={"token": response_data.get("token")})
    
    data = pd.DataFrame(response.json()['data'])
    other_data = pd.DataFrame(response.json()).drop(columns='data')
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN', 'UTC offset from (UTC+/-hhmm)',
                            'UTC offset to (UTC+/-hhmm)', 'content_name', 'content_id', 'timezone', 'unit'], inplace=True)
    
finally:

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"{zip_filename} silindi.")

    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
        print(f"{extract_dir} silindi.")

final_data.rename(columns={col: f"unlicensed_{col}" for col in final_data.columns[2:]}, inplace=True)
last_updated = final_data.groupby(['From yyyy-mm-dd hh:mm'], as_index=False)['unlicensed_update_id'].max()
unlicensed_updated_data = pd.merge(last_updated, final_data, on=['From yyyy-mm-dd hh:mm', 'unlicensed_update_id'])
for column in unlicensed_updated_data.columns[3:6]:
    unlicensed_updated_data[column] = unlicensed_updated_data[column].astype(int)
unlicensed_updated_data.drop(columns='unlicensed_update_id', inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

try:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1429/historical_data/{current_year}/{current_month}", # 1443
        params={"token": response_data.get("token")})

    zip_filename = 'historical_data_solarpv_licensed.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_solarpv_licensed"
    shutil.unpack_archive(zip_filename, extract_dir)
    print(f"{zip_filename} klasöre çıkarıldı: {extract_dir}")

    unlicensed_solarpv_data_files = [file for file in os.listdir('historical_data_solarpv_licensed') if 'post' not in file]

    data = []
    other_data = []

    for file in unlicensed_solarpv_data_files:
        df = pd.read_json(f'historical_data_solarpv_licensed/{file}')
        data.append(pd.DataFrame(list(df['data'].values)))
        other_data.append(df.drop(columns='data'))

    data = pd.concat(data, ignore_index=True)
    other_data = pd.concat(other_data, ignore_index=True)
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN', 'UTC offset from (UTC+/-hhmm)',
                            'UTC offset to (UTC+/-hhmm)', 'content_name', 'content_id', 'timezone', 'unit'], inplace=True)

except:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1429/data", # 1444
        params={"token": response_data.get("token")})
    
    data = pd.DataFrame(response.json()['data'])
    other_data = pd.DataFrame(response.json()).drop(columns='data')
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN', 'UTC offset from (UTC+/-hhmm)',
                            'UTC offset to (UTC+/-hhmm)', 'content_name', 'content_id', 'timezone', 'unit'], inplace=True)
    
finally:

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"{zip_filename} silindi.")

    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
        print(f"{extract_dir} silindi.")

final_data.rename(columns={col: f"licensed_{col}" for col in final_data.columns[2:]}, inplace=True)
last_updated = final_data.groupby(['From yyyy-mm-dd hh:mm'], as_index=False)['licensed_update_id'].max()
licensed_updated_data = pd.merge(last_updated, final_data, on=['From yyyy-mm-dd hh:mm', 'licensed_update_id'])
for column in licensed_updated_data.columns[3:6]:
    licensed_updated_data[column] = licensed_updated_data[column].astype(int)
licensed_updated_data.drop(columns='licensed_update_id', inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

try:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1446/historical_data/{current_year}/{current_month}", # 1443
        params={"token": response_data.get("token")})

    zip_filename = 'historical_data_wind.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_wind"
    shutil.unpack_archive(zip_filename, extract_dir)
    print(f"{zip_filename} klasöre çıkarıldı: {extract_dir}")

    wind_data_files = [file for file in os.listdir('historical_data_wind') if 'post' not in file]

    data = []
    other_data = []

    for file in wind_data_files:
        df = pd.read_json(f'historical_data_wind/{file}')
        data.append(pd.DataFrame(list(df['data'].values)))
        other_data.append(df.drop(columns='data'))

    data = pd.concat(data, ignore_index=True)
    other_data = pd.concat(other_data, ignore_index=True)
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN', 'UTC offset from (UTC+/-hhmm)',
                            'UTC offset to (UTC+/-hhmm)', 'content_name', 'content_id', 'timezone', 'unit'], inplace=True)

except:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1446/data", # 1444
        params={"token": response_data.get("token")})
    
    data = pd.DataFrame(response.json()['data'])
    other_data = pd.DataFrame(response.json()).drop(columns='data')
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['ARPEGE RUN', 'ECMWF ENS RUN', 'ECMWF HRES RUN', 'GFS RUN', 'UTC offset from (UTC+/-hhmm)',
                            'UTC offset to (UTC+/-hhmm)', 'content_name', 'content_id', 'timezone', 'unit'], inplace=True)
    
finally:

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"{zip_filename} silindi.")

    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
        print(f"{extract_dir} silindi.")

final_data.rename(columns={col: f"wind_{col}" for col in final_data.columns[2:]}, inplace=True)
last_updated = final_data.groupby(['From yyyy-mm-dd hh:mm'], as_index=False)['wind_update_id'].max()
wind_updated_data = pd.merge(last_updated, final_data, on=['From yyyy-mm-dd hh:mm', 'wind_update_id'])
for column in wind_updated_data.columns[3:6]:
    wind_updated_data[column] = wind_updated_data[column].astype(int)
wind_updated_data.drop(columns='wind_update_id', inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

try:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1459/historical_data/{current_year}/{current_month}",
        params={"token": response_data.get("token")})

    zip_filename = 'historical_data_conventional.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_conventional"
    shutil.unpack_archive(zip_filename, extract_dir)
    print(f"{zip_filename} klasöre çıkarıldı: {extract_dir}")

    conventional_data_files = [file for file in os.listdir('historical_data_conventional') if 'post' not in file]

    data = []
    other_data = []

    for file in conventional_data_files:
        df = pd.read_json(f'historical_data_conventional/{file}')
        data.append(pd.DataFrame(list(df['data'].values)))
        other_data.append(df.drop(columns='data'))

    data = pd.concat(data, ignore_index=True)
    other_data = pd.concat(other_data, ignore_index=True)
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['UTC offset from (UTC+/-hhmm)','UTC offset to (UTC+/-hhmm)',
                         'content_name','content_id','timezone','unit'], inplace=True)

except:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1459/data",
        params={"token": response_data.get("token")})
    
    data = pd.DataFrame(response.json()['data'])
    other_data = pd.DataFrame(response.json()).drop(columns='data')
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['UTC offset from (UTC+/-hhmm)','UTC offset to (UTC+/-hhmm)',
                         'content_name','content_id','timezone','unit'], inplace=True)
    
finally:

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"{zip_filename} silindi.")

    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
        print(f"{extract_dir} silindi.")

final_data.rename(columns={col: f"conventional_{col}" for col in final_data.columns[2:]}, inplace=True)
last_updated = final_data.groupby(['From yyyy-mm-dd hh:mm'], as_index=False)['conventional_update_id'].max()
conventional_updated_data = pd.merge(last_updated, final_data, on=['From yyyy-mm-dd hh:mm', 'conventional_update_id'])
conventional_updated_data['conventional_forecast'] = conventional_updated_data['conventional_forecast'].astype(int)
conventional_updated_data.drop(columns='conventional_update_id', inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

try:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1463/historical_data/{current_year}/{current_month}",
        params={"token": response_data.get("token")})

    zip_filename = 'historical_data_runofriver.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_runofriver"
    shutil.unpack_archive(zip_filename, extract_dir)
    print(f"{zip_filename} klasöre çıkarıldı: {extract_dir}")

    runofriver_data_files = [file for file in os.listdir('historical_data_runofriver') if 'post' not in file]

    data = []
    other_data = []

    for file in runofriver_data_files:
        df = pd.read_json(f'historical_data_runofriver/{file}')
        data.append(pd.DataFrame(list(df['data'].values)))
        other_data.append(df.drop(columns='data'))

    data = pd.concat(data, ignore_index=True)
    other_data = pd.concat(other_data, ignore_index=True)
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['UTC offset from (UTC+/-hhmm)','UTC offset to (UTC+/-hhmm)',
                         'content_name','content_id','timezone','unit'], inplace=True)

except:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1463/data",
        params={"token": response_data.get("token")})
    
    data = pd.DataFrame(response.json()['data'])
    other_data = pd.DataFrame(response.json()).drop(columns='data')
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['UTC offset from (UTC+/-hhmm)','UTC offset to (UTC+/-hhmm)',
                         'content_name','content_id','timezone','unit'], inplace=True)
    
finally:

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"{zip_filename} silindi.")

    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
        print(f"{extract_dir} silindi.")

final_data.rename(columns={col: f"runofriver_{col}" for col in final_data.columns[2:]}, inplace=True)
last_updated = final_data.groupby(['From yyyy-mm-dd hh:mm'], as_index=False)['runofriver_update_id'].max()
runofriver_updated_data = pd.merge(last_updated, final_data, on=['From yyyy-mm-dd hh:mm', 'runofriver_update_id'])
runofriver_updated_data['runofriver_forecast'] = runofriver_updated_data['runofriver_forecast'].astype(int)
runofriver_updated_data.drop(columns='runofriver_update_id', inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

try:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1455/historical_data/{current_year}/{current_month}",
        params={"token": response_data.get("token")})

    zip_filename = 'historical_data_power_demand.zip'
    with open(zip_filename, 'wb') as file:
        file.write(response.content)

    extract_dir = f"historical_data_power_demand"
    shutil.unpack_archive(zip_filename, extract_dir)
    print(f"{zip_filename} klasöre çıkarıldı: {extract_dir}")

    power_demand_data_files = [file for file in os.listdir('historical_data_power_demand') if 'post' not in file]

    data = []
    other_data = []

    for file in power_demand_data_files:
        df = pd.read_json(f'historical_data_power_demand/{file}')
        data.append(pd.DataFrame(list(df['data'].values)))
        other_data.append(df.drop(columns='data'))

    data = pd.concat(data, ignore_index=True)
    other_data = pd.concat(other_data, ignore_index=True)
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['UTC offset from (UTC+/-hhmm)','UTC offset to (UTC+/-hhmm)',
                         'content_name','content_id','timezone','unit'], inplace=True)

except:

    response = requests.get(
        url=f"https://api-markets.meteologica.com/api/v1/contents/1455/data",
        params={"token": response_data.get("token")})
    
    data = pd.DataFrame(response.json()['data'])
    other_data = pd.DataFrame(response.json()).drop(columns='data')
    final_data = pd.concat([data, other_data], axis=1)
    final_data.drop(columns=['UTC offset from (UTC+/-hhmm)','UTC offset to (UTC+/-hhmm)',
                         'content_name','content_id','timezone','unit'], inplace=True)
    
finally:

    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"{zip_filename} silindi.")

    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
        print(f"{extract_dir} silindi.")

final_data.rename(columns={col: f"demand_{col}" for col in final_data.columns[2:]}, inplace=True)
last_updated = final_data.groupby(['From yyyy-mm-dd hh:mm'], as_index=False)['demand_update_id'].max()
demand_updated_data = pd.merge(last_updated, final_data, on=['From yyyy-mm-dd hh:mm', 'demand_update_id'])
demand_updated_data['demand_forecast'] = demand_updated_data['demand_forecast'].astype(int)
demand_updated_data.drop(columns='demand_update_id', inplace=True)

#---------------------------------------------------------------------------------------------------------------------------------

sb_user = os.getenv('SUPABASE_USER')
sb_password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{sb_user}:{sb_password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

tables = {
    "unlicensed_solar": unlicensed_updated_data,
    "licensed_solar": licensed_updated_data,
    "wind": wind_updated_data,
    "dam_hydro": conventional_updated_data,
    "runofriver_hydro": runofriver_updated_data,
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

print(f"{datetime.now(turkey_timezone).isoformat()} data was uploaded to DB!")
print("Succeed!")