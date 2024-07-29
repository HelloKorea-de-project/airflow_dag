from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import timedelta, datetime
import pytz
import io

import logging
import pandas as pd
import pyarrow.parquet as pq

# 로깅 설정
logging.basicConfig(level=logging.INFO)


def get_area_code_from_raw(**kwargs):
    bucket = 'hellokorea-raw-layer'

    # define target key
    s3_key = f'source/tour/dimension/seoul_area_code.csv'

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    target_key = s3.read_key(
        key=s3_key,
        bucket_name=bucket
    )

    return target_key


def load_to_s3_stage(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-stage-layer'
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)

    # # Pull DataFrame from XCom
    csv_string = ti.xcom_pull(task_ids='get_area_code_from_raw')
    area_code_df = pd.read_csv(io.BytesIO(csv_string.encode('utf-8')))

    # add korName col
    area_code_df['korName'] = ['강남구', '강동구', '강북구', '강서구', '관악구', '광진구', '구로구', '금천구', '노원구', '도봉구',
                               '동대문구', '동작구', '마포구', '서대문구', '서초구', '성동구', '성북구', '송파구', '양천구', '영등포구',
                               '용산구', '은평구', '종로구', '중구', '중랑구']
    logging.info(area_code_df)
    logging.info(area_code_df.columns, area_code_df.dtypes)

    # add created and updated at timestamp columns
    createdAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    updateAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    area_code_df['createdAt'] = createdAt
    area_code_df['updatedAt'] = updateAt
    logging.info(area_code_df)
    logging.info(area_code_df.columns, area_code_df.dtypes)

    # Convert DataFrame to parquet bytes
    pq_bytes = convert_to_parquet_bytes(area_code_df)

    # Define S3 path
    s3_key = f'source/tour/dimension/seoul_area_code.parquet'
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload parquet file to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_to_s3(s3, s3_key, pq_bytes, bucket)


def copy_to_redshift(**kwargs):
    cur = get_Redshift_connection(autocommit=False)
    # drop and create table
    flush_table_sql = """DROP TABLE IF EXISTS dimension_data.seoul_area_code;
    CREATE TABLE dimension_data.seoul_area_code (
        rnum bigint,
        code bigint primary key,
        name varchar(32),
        korName varchar(32),
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
    s3_key = f'source/tour/dimension/seoul_area_code.parquet'
    logging.info(s3_key)

    # copy parquet file to redshift raw_data schema
    iam_role = Variable.get('hellokorea_redshift_s3_access_role')
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    copy_sql = f"""
        COPY dimension_data.seoul_area_code
        FROM 's3://{stage_bucket}/{s3_key}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """
    try:
        cur.execute(copy_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute('ROLLBACK;')
        raise


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


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


# Define default_args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'stage_area_code_data',
    default_args=default_args,
    description='A DAG to stage tour dimension area code data every day and save it to S3',
    schedule_interval='@once', # trigger manually when dimension file has been changed
    start_date=days_ago(1),
    catchup=False,
)

get_area_code_from_raw = PythonOperator(
    task_id='get_area_code_from_raw',
    python_callable=get_area_code_from_raw,
    dag=dag,
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

get_area_code_from_raw >> load_to_s3_stage >> copy_to_redshift