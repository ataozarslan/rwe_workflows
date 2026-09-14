import os
import sys
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from darts import TimeSeries
from darts.metrics import wmape, rmse, r2_score
from loguru import logger

user = os.getenv('SUPABASE_USER')
password = os.getenv('SUPABASE_PASSWORD')

connection_str = f"postgresql+psycopg2://{user}:{password}@aws-0-us-east-2.pooler.supabase.com:5432/postgres"
engine = create_engine(connection_str)

turkey_timezone = timezone(timedelta(hours=3))
today_start = datetime.now(turkey_timezone).replace(hour=0, minute=0, second=0, microsecond=0)

# Loguru configuration
logger.remove()

logger.level("INFO",     color="<blue>")
logger.level("SUCCESS",  color="<green>")
logger.level("WARNING",  color="<yellow>")
logger.level("ERROR",    color="<red>")
logger.level("CRITICAL", color="<bold><red>")

def format_with_tz(record):
    record["extra"]["tz_time"] = record["time"].astimezone(turkey_timezone).strftime("%Y-%m-%d %H:%M:%S")
    return "<level>{extra[tz_time]} | {level: <8} | {message}</level>\n"

logger.add(
    sys.stderr,
    format=format_with_tz,
    colorize=True,
    level="DEBUG"
)

# Validation period selection
validation_period = 2*168
validation_weeks = round(validation_period / 168)
validation_days_interval = validation_weeks * 7


query = text("""
SELECT date, price AS actual_price
FROM epias.ptf
    WHERE 
        date >= CURRENT_DATE - (:days * INTERVAL '1 day')
        AND date < CURRENT_DATE + INTERVAL '1 day'
        AND (
            (EXTRACT(DOW FROM CURRENT_DATE) = 0 AND EXTRACT(DOW FROM date) = 0)
            OR 
            (EXTRACT(DOW FROM CURRENT_DATE) != 0 AND EXTRACT(DOW FROM date) != 0)
        )
""").bindparams(days=validation_days_interval)

with engine.connect() as conn:
    ptf_df = pd.read_sql(query, con=conn)
    
ptf_df['actual_price'] = ptf_df['actual_price'].apply(lambda x: 1 if x <= 0 else x)

logger.info("Realized MCP data fetched...")

# -------------------------------------------------------------------- D+1 --------------------------------------------------------------------

query = text("""
SELECT date, min_price AS meteologica_min, avg_price AS meteologica_avg, max_price AS meteologica_max
FROM public.meteologica_forecast
    WHERE 
        date >= CURRENT_DATE - (:days * INTERVAL '1 day')
        AND date < CURRENT_DATE + INTERVAL '1 day'
        AND (
            (EXTRACT(DOW FROM CURRENT_DATE) = 0 AND EXTRACT(DOW FROM date) = 0)
            OR 
            (EXTRACT(DOW FROM CURRENT_DATE) != 0 AND EXTRACT(DOW FROM date) != 0)
        )
""").bindparams(days=validation_days_interval)

with engine.connect() as conn:
    meteologica_forecast = pd.read_sql(query, con=conn)

logger.info("Meteologica D+1 forecast data fetched...")

price_df = pd.merge(ptf_df, meteologica_forecast, on='date', how='left').sort_values(by='date')

query = text("""
SELECT date, low_price AS model_low, best_price AS model_best, high_price AS model_high
FROM public.model_forecast_ptf
    WHERE 
        date >= CURRENT_DATE - (:days * INTERVAL '1 day')
        AND date < CURRENT_DATE + INTERVAL '1 day'
        AND (
            (EXTRACT(DOW FROM CURRENT_DATE) = 0 AND EXTRACT(DOW FROM date) = 0)
            OR 
            (EXTRACT(DOW FROM CURRENT_DATE) != 0 AND EXTRACT(DOW FROM date) != 0)
        )
""").bindparams(days=validation_days_interval)

with engine.connect() as conn:
    model_forecast = pd.read_sql(query, con=conn)

logger.info("Model D+1 forecast data fetched...")

price_df = pd.merge(price_df, model_forecast, on='date', how='left')

hourly_valid_dict = {}

pred_cols = [col for col in price_df.columns if col not in ['date', 'actual_price']]

for hour in price_df['date'].dt.hour.unique():
    hourly_df = price_df[price_df['date'].dt.hour == hour]
    hourly_series = TimeSeries.from_dataframe(hourly_df, 'date', freq='h')
    
    actual = hourly_series['actual_price']

    hourly_valid_dict[hour] = {}
    
    for col in pred_cols:
        pred = hourly_series[col]
        hourly_valid_dict[hour][col] = {
            'wmape': np.round(wmape(actual, pred), 2),
            'rmse': np.round(rmse(actual, pred), 2),
            'r2': np.round(r2_score(actual, pred), 2)
        }

logger.success(f"D+1 hourly forecast validation was done for {round(validation_period/168)} weeks on {price_df['date'].max().date()}")

rows = []
for hour, models in hourly_valid_dict.items():

    run_id = f"{today_start.strftime('%Y%m%d')}H{hour:02d}_{round(validation_period/168)}W"
    row_dict = {
        'run_id': run_id,
        'hour': f'{hour:02d}:00'
    }
    for model_name, metrics in models.items():
        for metric_name, value in metrics.items():
            column_name = f"{model_name}_{metric_name}"
            row_dict[column_name] = value
    rows.append(row_dict)

metrics_df = pd.DataFrame(rows).set_index('run_id')

logger.success(f"D+1 hourly forecast metrics table was created successfully on {price_df['date'].max().date()}")

try:

    with engine.connect() as conn:

        metrics_df.to_sql("ptf_forecast_metrics", conn, if_exists="append", index=True, schema="public")

        logger.success(f"D+1 validation results for {price_df['date'].max().date()} was uploaded into database")

except Exception as e:

    logger.warning(f"D+1 validation results for {price_df['date'].max().date()} has already loaded into database!: {e}")

# -------------------------------------------------------------------- D+2 --------------------------------------------------------------------

query = text("""
SELECT date, min_price AS meteologica_min, avg_price AS meteologica_avg, max_price AS meteologica_max
FROM public."meteologica_forecast_d+2"
    WHERE 
        date >= CURRENT_DATE - (:days * INTERVAL '1 day')
        AND date < CURRENT_DATE + INTERVAL '1 day'
        AND (
            (EXTRACT(DOW FROM CURRENT_DATE) = 0 AND EXTRACT(DOW FROM date) = 0)
            OR 
            (EXTRACT(DOW FROM CURRENT_DATE) != 0 AND EXTRACT(DOW FROM date) != 0)
        )
""").bindparams(days=validation_days_interval)

with engine.connect() as conn:
    meteologica_forecast = pd.read_sql(query, con=conn)

logger.info("Meteologica D+2 forecast data fetched...")

price_df = pd.merge(ptf_df, meteologica_forecast, on='date', how='left').sort_values(by='date')

query = text("""
SELECT date, low_price AS model_low, best_price AS model_best, high_price AS model_high
FROM public.model_forecast_sfc
    WHERE 
        date >= CURRENT_DATE - (:days * INTERVAL '1 day')
        AND date < CURRENT_DATE + INTERVAL '1 day'
        AND (
            (EXTRACT(DOW FROM CURRENT_DATE) = 0 AND EXTRACT(DOW FROM date) = 0)
            OR 
            (EXTRACT(DOW FROM CURRENT_DATE) != 0 AND EXTRACT(DOW FROM date) != 0)
        )
""").bindparams(days=validation_days_interval)

with engine.connect() as conn:
    model_forecast = pd.read_sql(query, con=conn)

logger.info("Model D+2 forecast data fetched...")

price_df = pd.merge(price_df, model_forecast, on='date', how='left')

hourly_valid_dict = {}

pred_cols = [col for col in price_df.columns if col not in ['date', 'actual_price']]

for hour in price_df['date'].dt.hour.unique():
    hourly_df = price_df[price_df['date'].dt.hour == hour]
    hourly_series = TimeSeries.from_dataframe(hourly_df, 'date', freq='h')
    
    actual = hourly_series['actual_price']

    hourly_valid_dict[hour] = {}
    
    for col in pred_cols:
        pred = hourly_series[col]
        hourly_valid_dict[hour][col] = {
            'wmape': np.round(wmape(actual, pred), 2),
            'rmse': np.round(rmse(actual, pred), 2),
            'r2': np.round(r2_score(actual, pred), 2)
        }

logger.success(f"D+2 hourly forecast validation was done for {round(validation_period/168)} weeks on {price_df['date'].max().date()}")

rows = []
for hour, models in hourly_valid_dict.items():

    run_id = f"{today_start.strftime('%Y%m%d')}H{hour:02d}_{round(validation_period/168)}W"
    row_dict = {
        'run_id': run_id,
        'hour': f'{hour:02d}:00'
    }
    for model_name, metrics in models.items():
        for metric_name, value in metrics.items():
            column_name = f"{model_name}_{metric_name}"
            row_dict[column_name] = value
    rows.append(row_dict)

metrics_df = pd.DataFrame(rows).set_index('run_id')

logger.success(f"D+2 hourly forecast metrics table was created successfully on {price_df['date'].max().date()}")

try:

    with engine.connect() as conn:

        metrics_df.to_sql("sfc_forecast_metrics", conn, if_exists="append", index=True, schema="public")

        logger.success(f"D+2 validation results for {price_df['date'].max().date()} was uploaded into database")

except Exception as e:

    logger.warning(f"D+2 validation results for {price_df['date'].max().date()} has already loaded into database!: {e}")

logger.success("Pipeline was completed successfully...")
