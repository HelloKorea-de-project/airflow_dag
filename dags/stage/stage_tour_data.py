from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import timedelta, datetime
import pytz
import io

import requests
import logging
import pandas as pd
import pyarrow.parquet as pq
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO)


def get_tour_data_from_raw(**kwargs):
    bucket = 'hellokorea-raw-layer'

    # define target key
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    s3_key = f'source/tour/attractions/{kst_date.year}/{kst_date.month}/{kst_date.day}/tour_attractions_{kst_date.strftime("%Y%m%d")}.csv'

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    target_key = s3.read_key(
        key=s3_key,
        bucket_name=bucket
    )

    return target_key


def transform(**kwargs):
    ti = kwargs['ti']

    # convert csv-string to dataframe
    csv_string = ti.xcom_pull(task_ids='get_tour_data_from_raw')
    df = pd.read_csv(io.BytesIO(csv_string.encode('utf-8')))

    logging.info(f'{len(df)} records are read')
    logging.info(f'Columns: {df.columns}')

    # drop duplicated records with 'contentid' column
    unique_df = drop_duplicates(df, 'contentid')
    # drop 'areacode' column
    col_dropped_df = drop_column(unique_df, 'areacode')
    # convert time string to datetime format
    convert_time_format(col_dropped_df, 'createdtime', 'createdAt')
    convert_time_format(col_dropped_df, 'modifiedtime', 'updatedAt')

    # drop original createdtime, modifiedtime cols
    createdtime_dropped = drop_column(col_dropped_df, 'createdtime')
    modifiedtime_dropped = drop_column(createdtime_dropped, 'modifiedtime')

    return modifiedtime_dropped.to_json(date_format='iso', date_unit='s')
def load_to_s3_stage(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-stage-layer'

    # # Pull merged DataFrame from XCom
    tour_data_json = ti.xcom_pull(task_ids='transform')
    df_tour = pd.read_json(tour_data_json)
    logging.info(df_tour)

    # Convert DataFrame to parquet bytes
    pq_bytes = convert_to_parquet_bytes(df_tour)

    # Define S3 path
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    s3_key = f'source/tour/attractions/{kst_date.year}/{kst_date.month}/{kst_date.day}/tour_attractions_{kst_date.strftime("%Y%m%d")}.parquet'
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload parquet file to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_to_s3(s3, s3_key, pq_bytes, bucket)


def drop_duplicates(df, criteria_col):
    logging.info(f'total data length before dropping : {len(df)}')
    dropped = df.drop_duplicates(subset=[criteria_col])
    logging.info(f'total data length before dropping : {len(dropped)}')
    logging.info(f'After drop records: {dropped.columns}')
    return dropped


def drop_column(df, target_col):
    dropped = df.drop(target_col, axis='columns')
    logging.info(f'After drop columns: {dropped.columns}')
    return dropped


def convert_time_format(df, target_col, new_col):
    time_converted = df[target_col].apply(lambda date_str: datetime.strptime(str(date_str), '%Y%m%d%H%M%S'))
    logging.info(f'Time converted series: {time_converted}')
    df[new_col] = time_converted
    logging.info(f'After {target_col} col converted: {df.columns}')



def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)


def convert_to_parquet_bytes(df):
    pq_buffer = io.BytesIO()
    df.to_parquet(pq_buffer, index=False)
    return pq_buffer.getvalue()


def upload_to_s3(s3, s3_key, pq_bytes, bucket):
    s3.load_bytes(
        bytes_data=pq_bytes,
        key=s3_key,
        bucket_name=bucket
    )


# Define default_args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'stage_tour_attractions_data',
    default_args=default_args,
    description='A DAG to stage tour attractions data every day and save it to S3',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
)

get_tour_data_from_raw = PythonOperator(
    task_id='get_tour_data_from_raw',
    python_callable=get_tour_data_from_raw,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_to_s3_stage = PythonOperator(
    task_id='load_to_s3_stage',
    python_callable=load_to_s3_stage,
    dag=dag,
)

get_tour_data_from_raw >> transform >> load_to_s3_stage