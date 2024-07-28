from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import timedelta, datetime
import pytz
import io
from sqlalchemy import create_engine

import logging
import pandas as pd
import pyarrow.parquet as pq

# 로깅 설정
logging.basicConfig(level=logging.INFO)


def get_area_code_s3_key(**kwargs):
    """
    get s3 key to load to RDS SeoulAreaCode table
    :param kwargs:
    :return:
    """
    # define target key
    s3_key = f'source/tour/dimension/seoul_area_code.parquet'
    logging.info(s3_key)
    return s3_key


def ensure_integrity(**kwargs):
    """
    ensure data integrity before load table to rds
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    bucket = 'hellokorea-stage-layer'

    # get s3 key to extract joined table before task
    code_s3_key = ti.xcom_pull(task_ids='get_area_code_s3_key')
    logging.info(code_s3_key)

    # get area code parquet file
    s3 = S3Hook('s3_conn')
    area_code_file_stream = s3.get_key(
        key=code_s3_key,
        bucket_name=bucket
    )

    # read parquet
    byte_buffer = io.BytesIO(area_code_file_stream.get()["Body"].read())
    logging.info(f"parquet schema: {pq.read_schema(byte_buffer)}")
    df = pd.read_parquet(byte_buffer)

    # pick only necessary columns
    df = df[['code', 'name', 'korName']]
    logging.info(f"Dataframe cols: {df.columns}")
    logging.info(f"Dataframe schema: {df.dtypes}")

    # drop duplicate by pk: contentid
    unique_df = drop_duplicates(df, 'code')

    # manage NaN values
    result_df = manage_null(unique_df)

    # change format 'code' column to int
    result_df['code'] = result_df['code'].astype('int')

    # define key to load to temp zone
    bucket = 'hellokorea-external-zone'
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    s3_temp_key = f'source/tour/dimension/ensured_seoul_area_code.parquet'

    # load to s3 temp zone
    pq_bytes = convert_to_parquet_bytes(result_df)
    upload_to_s3(s3, s3_temp_key, pq_bytes, bucket)

    return s3_temp_key


def load_to_rds(**kwargs):
    """
    load tour attraction info in rds 'tour_seoultourinfo' table
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    bucket = 'hellokorea-external-zone'

    # get s3 key to extract joined table before task
    s3_temp_key = ti.xcom_pull(task_ids='ensure_integrity')
    # get joined parquet file
    s3 = S3Hook('s3_conn')
    s3_file_obj = s3.get_key(
        key=s3_temp_key,
        bucket_name=bucket
    )

    # read parquet
    byte_buffer = io.BytesIO(s3_file_obj.get()["Body"].read())
    logging.info(f"parquet schema: {pq.read_schema(byte_buffer)}")
    df = pd.read_parquet(byte_buffer)
    logging.info(f"Dataframe cols: {df.columns}")
    logging.info(f"Dataframe schema: {df.dtypes}")

    # delete temp joined file in s3
    delete_file_in_s3(s3, s3_temp_key, bucket)

    # connect to rds
    engine = connect_to_rds()

    # load to rds
    df.to_sql('tour_seoulareacode', con=engine,if_exists='replace', index=False)

    # close connections
    engine.dispose()

def connect_to_rds():
    conn_str = Variable.get('rds_test_db_conn')
    return create_engine(conn_str)


def manage_null(df):
    # Raise exception when code is Null
    if len(df[df['code'].isnull()]) > 0:
        raise ValueError("Null value has benn detected in 'code' column")
    if len(df[df['name'].isnull()]) > 0:
        raise ValueError("Null value has benn detected in 'name' column")
    if len(df[df['korName'].isnull()]) > 0:
        raise ValueError("Null value has benn detected in 'korName' column")

    return df


def drop_duplicates(df, criteria_col):
    logging.info(f'total data length before dropping : {len(df)}')
    dropped = df.drop_duplicates(subset=[criteria_col])
    logging.info(f'total data length before dropping : {len(dropped)}')
    logging.info(f'After drop records: {dropped.columns}')
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
    df.to_parquet(pq_buffer, engine='pyarrow', use_deprecated_int96_timestamps=True, index=False)
    logging.info(pq.read_schema(pq_buffer))
    return pq_buffer.getvalue()


def upload_to_s3(s3, s3_key, pq_bytes, bucket):
    s3.load_bytes(
        bytes_data=pq_bytes,
        key=s3_key,
        bucket_name=bucket
    )


def delete_file_in_s3(s3, s3_key, bucket):
    s3.delete_objects(
        keys=s3_key,
        bucket=bucket
    )


# Define default_args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'produce_arae_code',
    default_args=default_args,
    description='A DAG to load seoul area code data to rds every day',
    schedule_interval='@once', # triggered by stage_area_code every day
    start_date=days_ago(1),
    catchup=False,
)

get_area_code_s3_key = PythonOperator(
    task_id='get_area_code_s3_key',
    python_callable=get_area_code_s3_key,
    dag=dag,
)

ensure_integrity = PythonOperator(
    task_id='ensure_integrity',
    python_callable=ensure_integrity,
    dag=dag,
)

load_to_rds = PythonOperator(
    task_id='load_to_rds',
    python_callable=load_to_rds,
    dag=dag,
)

get_area_code_s3_key>> ensure_integrity >> load_to_rds