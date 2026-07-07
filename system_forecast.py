import os
import time
import requests
import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
from sklearn.metrics import root_mean_squared_error, r2_score
from darts import TimeSeries
from chronos import BaseChronosPipeline, Chronos2Pipeline
from loguru import logger

epias_username = os.getenv('EPIAS_USERNAME')
epias_password = os.getenv('EPIAS_PASSWORD')

user = os.getenv("SUPABASE_USER")
password = os.getenv("SUPABASE_PASSWORD")

connection_str = f"postgresql+psycopg2://{user}:{password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

turkey_timezone = timezone(timedelta(hours=3))
today_start = datetime.now(turkey_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
d1_start = today_start - timedelta(days=1)
end_time = datetime.now(turkey_timezone) - timedelta(hours=1)

#--------------------------------------------- Data Preparation -----------------------------------------------------------

query = """
SELECT u."From-yyyy-mm-dd-hh-mm" AS date, d.demand_forecast AS demand, w.wind_forecast AS wind,
dam.conventional_forecast + r.runofriver_forecast AS hydro,
u.unlicensed_forecast + l.licensed_forecast AS solar
FROM meteologica.unlicensed_solar u
JOIN meteologica.licensed_solar l ON u."From-yyyy-mm-dd-hh-mm" = l."From-yyyy-mm-dd-hh-mm"
JOIN meteologica.wind w on u."From-yyyy-mm-dd-hh-mm" = w."From-yyyy-mm-dd-hh-mm"
JOIN meteologica.dam_hydro dam on u."From-yyyy-mm-dd-hh-mm" = dam."From-yyyy-mm-dd-hh-mm"
JOIN meteologica.runofriver_hydro r on u."From-yyyy-mm-dd-hh-mm" = r."From-yyyy-mm-dd-hh-mm"
JOIN meteologica.demand d on u."From-yyyy-mm-dd-hh-mm" = d."From-yyyy-mm-dd-hh-mm"
ORDER BY 1
"""

generation_df = pd.read_sql(query, con=engine)
generation_df['date'] = pd.to_datetime(generation_df['date']).dt.tz_localize(None)

query = """
SELECT ptf.date,
DATE(ptf.date) as day,
smf."systemMarginalPrice" AS smf,
ptf.price AS ptf,
smf."systemMarginalPrice" - ptf.price AS smf_ptf_diff
FROM epias.smf
RIGHT JOIN epias.ptf ON smf.date = ptf.date
ORDER BY 1
"""

smf_ptf_df = pd.read_sql(query, con=engine)

query = """
WITH filtered_messages AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY "uevcbName", CAST("caseStartDate" AS DATE)
                ORDER BY "caseEndDate" DESC
            ) AS row_num
        FROM epias.market_messages
    ) t
    WHERE t.row_num = 1
),

exploded AS (
    SELECT
        CAST(generate_series(
            CAST("caseStartDate" AS DATE),
            CAST("caseEndDate" AS DATE),
            INTERVAL '1 day'
        ) AS DATE) AS active_day,
        "uevcbName",
        "operatorPower",
        "capacityAtCaseTime"
    FROM filtered_messages
),

exploded_deduped AS (
    SELECT
        active_day,
        "uevcbName",
        MAX("operatorPower")       AS "operatorPower",
        MAX("capacityAtCaseTime")  AS "capacityAtCaseTime"
    FROM exploded
    GROUP BY active_day, "uevcbName"
),

daily_failure AS (
    SELECT
        e.active_day                                            AS failure_day,
        SUM(e."operatorPower" - e."capacityAtCaseTime")         AS total_failure
    FROM exploded_deduped e
    GROUP BY e.active_day
)

SELECT
    df.failure_day,
    ROUND(df.total_failure)                                  AS failure_mw
FROM daily_failure df
ORDER BY df.failure_day ASC;
"""

market_messages = pd.read_sql(query, con=engine)

failure_df = pd.merge(smf_ptf_df, market_messages, left_on='day', right_on='failure_day', how='left')
failure_df.drop(columns=['failure_day','day'], inplace=True)
new_df = pd.merge(generation_df, failure_df, on='date', how='right')

query = f"""
SELECT date, net AS system_direction FROM epias.yal
WHERE date < '{d1_start.isoformat().split('T')[0]}'
"""

dgp_df = pd.read_sql(query, con=engine)
dgp_df.sort_values('date', inplace=True)
dgp_df.reset_index(drop=True, inplace=True)

url = "https://giris.epias.com.tr/cas/v1/tickets"

data = {
    "username":epias_username,
    "password":epias_password}

response_tgt = requests.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded","Accept": "text/plain"}, timeout=30)

if response_tgt.status_code == 201:
    tgt_code = response_tgt.text
else:
    logger.error(f"Error: {response_tgt.status_code}, Message: {response_tgt.text}")

def safe_post(url, json=None, headers=None, retries=3, timeout=60):
    for i in range(retries):
        try:
            response = requests.post(url, json=json, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response
            else:
                logger.error(f"[{i+1}/{retries}] HTTP {response.status_code} error, retrying...")
        except requests.exceptions.ReadTimeout:
            logger.error(f"[{i+1}/{retries}] ReadTimeout error, retrying...")
        except requests.exceptions.RequestException as e:
            logger.error(f"[{i+1}/{retries}] Other error: {e}, retrying...")
        time.sleep(5)
    raise Exception(f"Request to {url} failed after {retries} retries.")


service_url = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/bpm/data/order-summary-up"

response_url = safe_post(
    service_url,
    json={"startDate": str(d1_start.isoformat()),
        "endDate": str(today_start.isoformat())},
    headers={"Accept-Language":"en",
            "Accept":"application/json",
            "Content-Type":"application/json",
            "TGT":tgt_code},
    timeout=30
    )

if response_url.status_code == 200:
    response = response_url.json()
    logger.success(f"API call successful...")
else:
    logger.error(f"Error: {response_url.status_code}, Message: {response_url.text}")

updated_yal_yat = pd.DataFrame.from_records(response['items'])
updated_yal_yat = updated_yal_yat[updated_yal_yat['net'] != 0].copy()
updated_yal_yat = updated_yal_yat[['date', 'net']]
updated_yal_yat.columns = ['date', 'system_direction']
updated_yal_yat['date'] = pd.to_datetime(updated_yal_yat['date']).dt.tz_localize(None)

dgp_df = pd.concat([dgp_df, updated_yal_yat])
dgp_df['date'] = pd.to_datetime(dgp_df['date'])
df = pd.merge(new_df, dgp_df, on='date', how='left')
df.set_index('date', inplace=True)
ts_df = TimeSeries.from_dataframe(df)
ts_df = ts_df.add_holidays('TR')
df = ts_df.to_dataframe().rename(columns={'holidays': 'is_holiday'})
df['date_only'] = df.index.date
df['date_only'] = pd.to_datetime(df['date_only'])

query = "SELECT * FROM epias.ramadan_dates"

with engine.connect() as conn:
    ramadan_df = pd.read_sql(query, con=conn)
    
ramadan_df['date'] = pd.to_datetime(ramadan_df['date'])
merged_df = df.merge(ramadan_df, left_on="date_only", right_on="date", how="left").drop(columns=["date"]).drop(columns='date_only')
merged_df.index = df.index
df = merged_df.copy()

df['demand_renewable_diff'] = df['demand'] - df['wind'] - df['solar'] - df['hydro']
df['demand_diff24'] = df['demand'].diff(24)
df['hour'] = df.index.hour.astype(float)
hour_map = {hour: 'off-peak1' if hour < 10 else 'peak' if hour >= 18 else 'off-peak2' for hour in range(24)}
df['is_peak'] = df.index.hour.map(hour_map)
day_map = {day: 'Sunday' if day == 'Sunday' else 'Saturday' if day == 'Saturday' else 'Weekday' for day in df.index.day_name().unique()}
df['week_part'] = df.index.day_name().map(day_map)
df = pd.get_dummies(df, dtype='float')

df['system_direction_lag1'] = df['system_direction'].shift(1)
df['smf_lag168'] = df['smf'].shift(24*7)
df['smf_lag24'] = df['smf'].shift(24)
df['smf_ptf_diff_lag24'] = df['smf_ptf_diff'].shift(24)
df['system_direction_ma3'] = df['system_direction'].rolling(window=3).mean().shift(1)
df['system_direction_ma6'] = df['system_direction'].rolling(window=6).mean().shift(1)
df['system_direction_ma12'] = df['system_direction'].rolling(window=12).mean().shift(1)
df.drop(columns=['smf','smf_ptf_diff'], inplace=True)
df = df.iloc[168:].copy()

#------------------------------------------- Train/Test Splitting ---------------------------------------------------

covariates_df = df.drop(columns=['system_direction'])
covariates_df.reset_index(inplace=True)
covariates_df['id'] = 'DF'

df.reset_index(inplace=True)
df['id'] = 'DF'

train_val, test = df[:-df['system_direction'].isnull().sum()], df[-df['system_direction'].isnull().sum():]
train, val = train_val[train_val['date'] <= (train_val['date'].max() - pd.Timedelta(days=3))], train_val[train_val['date'] > (train_val['date'].max() - pd.Timedelta(days=3))]

logger.info(f"Validation Start Date: {val['date'].min()}, Validation End Date: {val['date'].max()}")

#--------------------------------------------- Validation Results ----------------------------------------------------

pipeline: Chronos2Pipeline = BaseChronosPipeline.from_pretrained("amazon/chronos-2", device_map="cpu")
logger.success(f"Pipeline loaded successfully...")

val_covariates = val.drop(columns=['system_direction'])

pred_dict = {f't+{i}': [] for i in range(1, 6)}
loop_no = 0

for day in range(len(val)):

    if len(val) - day < 5:

        loop_no += 1
        val_pred_df = pipeline.predict_df(
            train_val.iloc[:-len(val) + day], 
            val_covariates.iloc[day:5 + day - loop_no],
            id_column='id', timestamp_column='date', target='system_direction',
            prediction_length=5-loop_no, quantile_levels=np.arange(0.05, 1, 0.1).round(2).tolist()
        )

    else:
        val_pred_df = pipeline.predict_df(
            train_val.iloc[:-len(val) + day], 
            val_covariates.iloc[day:5 + day],
            id_column='id', timestamp_column='date', target='system_direction',
            prediction_length=5, quantile_levels=np.arange(0.05, 1, 0.1).round(2).tolist()
        )
    
    for i in range(5-loop_no):
        pred_dict[f't+{i+1}'].append(val_pred_df.iloc[[i]])

    if day % 18 == 0:
        progress_percentage = int((day / len(val)) * 100)
        print(f"{progress_percentage}% of the pipeline has been completed")
    
dfs = {step: pd.concat(rows, ignore_index=True) for step, rows in pred_dict.items()}

results_t1 = {}
logger.info(f"Validation Results for T+1")

quantile_cols = [col for col in val_pred_df.columns if col not in ['id', 'date', 'target_name', 'predictions']]

for q in quantile_cols:
    rmse = root_mean_squared_error(val['system_direction'], dfs['t+1'][q])
    r2 = r2_score(val['system_direction'], dfs['t+1'][q])
    results_t1[q] = {'rmse': rmse, 'r2': r2}
    logger.info(f"Quantile {q} -> RMSE: {round(rmse, 2)}, R-squared: {round(r2, 2)}")

best_q_t1 = min(results_t1, key=lambda x: results_t1[x]['rmse'])

results_t2 = {}
logger.info(f"Validation Results for T+2")

quantile_cols = [col for col in val_pred_df.columns if col not in ['id', 'date', 'target_name', 'predictions']]

for q in quantile_cols:
    rmse = root_mean_squared_error(val.iloc[1:]['system_direction'], dfs['t+2'][q])
    r2 = r2_score(val.iloc[1:]['system_direction'], dfs['t+2'][q])
    results_t2[q] = {'rmse': rmse, 'r2': r2}
    logger.info(f"Quantile {q} -> RMSE: {round(rmse, 2)}, R-squared: {round(r2, 2)}")

best_q_t2 = min(results_t2, key=lambda x: results_t2[x]['rmse'])

results_t3 = {}
logger.info(f"Validation Results for T+3")

quantile_cols = [col for col in val_pred_df.columns if col not in ['id', 'date', 'target_name', 'predictions']]

for q in quantile_cols:
    rmse = root_mean_squared_error(val.iloc[2:]['system_direction'], dfs['t+3'][q])
    r2 = r2_score(val.iloc[2:]['system_direction'], dfs['t+3'][q])
    results_t3[q] = {'rmse': rmse, 'r2': r2}
    logger.info(f"Quantile {q} -> RMSE: {round(rmse, 2)}, R-squared: {round(r2, 2)}")

best_q_t3 = min(results_t3, key=lambda x: results_t3[x]['rmse'])

results_t4 = {}
logger.info(f"Validation Results for T+4")

quantile_cols = [col for col in val_pred_df.columns if col not in ['id', 'date', 'target_name', 'predictions']]

for q in quantile_cols:
    rmse = root_mean_squared_error(val.iloc[3:]['system_direction'], dfs['t+4'][q])
    r2 = r2_score(val.iloc[3:]['system_direction'], dfs['t+4'][q])
    results_t4[q] = {'rmse': rmse, 'r2': r2}
    logger.info(f"Quantile {q} -> RMSE: {round(rmse, 2)}, R-squared: {round(r2, 2)}")

best_q_t4 = min(results_t4, key=lambda x: results_t4[x]['rmse'])

results_t5 = {}
logger.info(f"Validation Results for T+5")

quantile_cols = [col for col in val_pred_df.columns if col not in ['id', 'date', 'target_name', 'predictions']]

for q in quantile_cols:
    rmse = root_mean_squared_error(val.iloc[4:]['system_direction'], dfs['t+5'][q])
    r2 = r2_score(val.iloc[4:]['system_direction'], dfs['t+5'][q])
    results_t5[q] = {'rmse': rmse, 'r2': r2}
    logger.info(f"Quantile {q} -> RMSE: {round(rmse, 2)}, R-squared: {round(r2, 2)}")

best_q_t5 = min(results_t5, key=lambda x: results_t5[x]['rmse'])

best_q_results = pd.DataFrame({
    'date': d1_start.date(),
    't1_best_q': best_q_t1,
    't2_best_q': best_q_t2,
    't3_best_q': best_q_t3,
    't4_best_q': best_q_t4,
    't5_best_q': best_q_t5,
    't1_r2': round(results_t1[best_q_t1]['r2'], 2),
    't2_r2': round(results_t2[best_q_t2]['r2'], 2),
    't3_r2': round(results_t3[best_q_t3]['r2'], 2),
    't4_r2': round(results_t4[best_q_t4]['r2'], 2),
    't5_r2': round(results_t5[best_q_t5]['r2'], 2)
}, index=[0])

best_q_results['date'] = pd.to_datetime(best_q_results['date'])
for column in best_q_results.columns:
    if column.endswith('best_q'):
        best_q_results[column] = best_q_results[column].astype(float)

with engine.connect() as conn:

    best_q_results.to_sql('system_forecast_results', engine, if_exists='append', index=False, schema='public')
    logger.success(f"Model validation results loaded successfully into database!")
