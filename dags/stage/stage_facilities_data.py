from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import timedelta, datetime
import pytz
import io

import logging
import pandas as pd

# 로깅 설정
logging.basicConfig(level=logging.INFO)


def get_facilities_from_raw(**kwargs):
    bucket = 'hellokorea-raw-layer'

    # define target key
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    s3_key = f'source/kopis/facilities/{kst_date.year}/{kst_date.month}/{kst_date.day}/facilities_{kst_date.strftime("%Y%m%d")}.csv'
    logging.info(s3_key)

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    target_key = s3.read_key(
        key=s3_key,
        bucket_name=bucket
    )

    return target_key


def transform(**kwargs):
    ti = kwargs['ti']

    # Pull merged DataFrame from xcom
    facilities_csv_string = ti.xcom_pull(task_ids='get_facilities_from_raw')
    facilities_df = pd.read_csv(io.BytesIO(facilities_csv_string.encode('utf-8')))
    logging.info(facilities_df)

    # drop duplicate records using mt20id column
    unique_df = drop_duplicates(facilities_df, 'mt10id')

    # drop duplicated columns
    col_dropped_df = drop_column(
        unique_df,
        ['fcltynm', 'mt13cnt', 'fcltychartr', 'opende']
    )

    return col_dropped_df.to_json()

def load_to_s3_stage(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-stage-layer'
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)

    # # Pull merged DataFrame from XCom
    facilities_json = ti.xcom_pull(task_ids='transform')
    facilities_df = pd.read_json(facilities_json)
    logging.info(facilities_df)

    # add created and updated at timestamp columns
    createdAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    updateAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    facilities_df['createdAt'] = createdAt
    facilities_df['updatedAt'] = updateAt
    logging.info(facilities_df)
    logging.info(facilities_df.columns, facilities_df.dtypes)

    # Convert DataFrame to parquet bytes
    pq_bytes = convert_to_parquet_bytes(facilities_df)

    # Define S3 path
    s3_key = get_s3_key(execution_date)

    # Upload parquet file to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_to_s3(s3, s3_key, pq_bytes, bucket)


def copy_to_redshift(**kwargs):
    cur = get_Redshift_connection(autocommit=False)
    # drop and create table
    flush_table_sql = """DROP TABLE IF EXISTS raw_data.performance_facility_sidogu;
    CREATE TABLE raw_data.performance_facility_sidogu (
        mt10id varchar(256) primary key,
        sidonm varchar(256),
        gugunnm varchar(256),
        createdAt timestamp default GETDATE(),
        updatedAt timestamp default GETDATE()
    );
    """
    logging.info(flush_table_sql)
    try:
        cur.execute(flush_table_sql)
        cur.execute('COMMIT;')
    except Exception as e:
        cur.execute('ROLLBACK;')
        raise

    # define s3 url
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    execution_date = kwargs['execution_date']
    s3_key = get_s3_key(execution_date)

    # copy parquet file to redshift raw_data schema
    iam_role = Variable.get('hellokorea_redshift_s3_access_role')
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    copy_sql = f"""
        COPY raw_data.performance_facility_sidogu
        FROM 's3://{stage_bucket}/{s3_key}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """
    logging.info(copy_sql)
    try:
        cur.execute(copy_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute('ROLLBACK;')
        raise


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def drop_duplicates(df, criteria_col):
    logging.info(f'total data length before dropping : {len(df)}')
    dropped = df.drop_duplicates(subset=[criteria_col])
    logging.info(f'total data length after dropping : {len(dropped)}')
    logging.info(f'After drop records: {dropped.columns}')
    return dropped


def drop_column(df, target_col):
    dropped = df.drop(target_col, axis='columns')
    logging.info(f'After drop columns: {dropped.columns}')
    return dropped


def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)


def convert_to_parquet_bytes(df):
    pq_buffer = io.BytesIO()
    df.to_parquet(pq_buffer, index=False)
    return pq_buffer.getvalue()

def get_s3_key(execution_date):
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')

    return f'source/kopis/facilities/{kst_date.year}/{kst_date.month}/{kst_date.day}/facilities_{kst_date.strftime("%Y%m%d")}.parquet'


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
    'stage_facilities_data',
    default_args=default_args,
    description='A DAG to stage seoul facilities data every day and save it to S3',
    schedule_interval='@once',
    start_date=days_ago(7),
    catchup=False,
)

get_facilities_from_raw = PythonOperator(
    task_id='get_facilities_from_raw',
    python_callable=get_facilities_from_raw,
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

copy_to_redshift = PythonOperator(
    task_id='copy_to_redshift',
    python_callable=copy_to_redshift,
    dag=dag,
)

get_facilities_from_raw >> transform >> load_to_s3_stage >> copy_to_redshift