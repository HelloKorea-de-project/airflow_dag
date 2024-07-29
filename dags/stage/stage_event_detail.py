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
import pyarrow.parquet as pq

# 로깅 설정
logging.basicConfig(level=logging.INFO)


def get_performance_detail_from_raw(**kwargs):
    bucket = 'hellokorea-raw-layer'

    # define target key
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    s3_key = f'source/kopis/performance_detail/{kst_date.year}/{kst_date.month}/{kst_date.day}/performance_detail_{kst_date.strftime("%Y%m%d")}.csv'
    logging.info(s3_key)

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    target_key = s3.read_key(
        key=s3_key,
        bucket_name=bucket
    )

    return target_key


def get_festival_detail_from_raw(**kwargs):
    bucket = 'hellokorea-raw-layer'

    # define target key
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    s3_key = f'source/kopis/festival_detail/{kst_date.year}/{kst_date.month}/{kst_date.day}/festival_detail_{kst_date.strftime("%Y%m%d")}.csv'
    logging.info(s3_key)

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    target_key = s3.read_key(
        key=s3_key,
        bucket_name=bucket
    )

    return target_key


def merge_tables(**kwargs):
    ti = kwargs['ti']

    # convert performance csv-string to dataframe
    perf_csv_string = ti.xcom_pull(task_ids='get_performance_detail_from_raw')
    perf_df = pd.read_csv(io.BytesIO(perf_csv_string.encode('utf-8')))
    logging.info(f'{len(perf_df)} records are read')
    logging.info(f'Columns: {perf_df.columns}')

    # convert festival csv-string to dataframe
    fest_csv_string = ti.xcom_pull(task_ids='get_festival_detail_from_raw')
    fest_df = pd.read_csv(io.BytesIO(fest_csv_string.encode('utf-8')))
    logging.info(f'{len(fest_df)} records are read')
    logging.info(f'Columns: {fest_df.columns}')

    # merge performance and festival table
    event_df = pd.concat([perf_df, fest_df])
    event_df.reset_index(inplace=True)

    return event_df.to_json(index=False)


def transform(**kwargs):
    ti = kwargs['ti']

    # Pull merged DataFrame from xcom
    event_json = ti.xcom_pull(task_ids='merge_tables')
    event_df = pd.read_json(event_json)
    logging.info(event_df)

    # drop duplicate records using mt20id column
    unique_df = drop_duplicates(event_df, 'mt20id')
    # drop duplicated columns
    col_dropped_df = drop_column(unique_df, ['index'])
    # convert seat price to dict string
    convert_seatprice(col_dropped_df, 'seatprices')
    # drop original seatprice column
    result_df = drop_column(col_dropped_df, 'pcseguidance')

    return result_df.to_json()

def load_to_s3_stage(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-stage-layer'
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)

    # # Pull merged DataFrame from XCom
    event_json = ti.xcom_pull(task_ids='transform')
    event_df = pd.read_json(event_json)
    logging.info(event_df)


    # convert time value
    convert_time_format(event_df, 'prfpdfrom', 'eventstart')
    convert_time_format(event_df, 'prfpdto', 'eventend')

    # drop original time column
    prfpdfrom_dropped = drop_column(event_df, 'prfpdfrom')
    prfpdto_dropped = drop_column(prfpdfrom_dropped, 'prfpdto')

    # add created and updated at timestamp columns
    createdAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    updateAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    prfpdto_dropped['createdAt'] = createdAt
    prfpdto_dropped['updatedAt'] = updateAt
    logging.info(prfpdto_dropped)
    logging.info(prfpdto_dropped.columns, prfpdto_dropped.dtypes)

    # Convert DataFrame to parquet bytes
    pq_bytes = convert_to_parquet_bytes(prfpdto_dropped)

    # Define S3 path
    s3_key = get_s3_key(execution_date)

    # Upload parquet file to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_to_s3(s3, s3_key, pq_bytes, bucket)


def copy_to_redshift(**kwargs):
    cur = get_Redshift_connection(autocommit=False)
    # drop and create table
    flush_table_sql = """DROP TABLE IF EXISTS raw_data.event_detail;
    CREATE TABLE raw_data.event_detail (
        mt20id varchar(256) primary key,
        prfnm varchar(256),
        fcltynm varchar(256),
        prfcast varchar(256),
        prfcrew varchar(256),
        prfruntime varchar(32),
        prfage varchar(32),
        entrpsnm varchar(256),
        poster varchar(256),
        sty varchar(65535),
        genrenm varchar(32),
        openrun varchar(1),
        prfstate varchar(32),
        mt10id varchar(256),
        dtguidance varchar(2048),
        seatprice varchar(256),
        eventstart varchar(32),
        eventend varchar(32),
        createdAt timestamp,
        updatedAt timestamp
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
        COPY raw_data.event_detail
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


def convert_time_format(df, target_col, new_col):
    time_converted = df[target_col].apply(
        lambda date_str:
            datetime.strptime(str(date_str), '%Y.%m.%d')
                .strftime('%Y-%m-%d'),
    )
    logging.info(f'Time converted series: {time_converted}')
    df[new_col] = time_converted
    logging.info(f'After {target_col} col converted: {df.columns}')


def convert_seatprice(df, newcol):
    target_col = 'pcseguidance'
    df[newcol] = df[target_col].apply(convert_to_dict)

def convert_to_dict(data):
    prices = data.split(', ')
    if prices[0].startswith('전석무료'):
        return "{'전석': '0원'}"
    price_dict = {}
    for info in prices:
        seat_info = info.split(' ')
        price_dict[seat_info[0]] = seat_info[1]
    return f'{price_dict}'


def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)


def convert_to_parquet_bytes(df):
    pq_buffer = io.BytesIO()
    df.to_parquet(pq_buffer, index=False)
    logging.info(pq.read_schema(pq_buffer))
    return pq_buffer.getvalue()


def get_s3_key(execution_date):
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')

    return f'source/kopis/event_detail/{kst_date.year}/{kst_date.month}/{kst_date.day}/event_detail_{kst_date.strftime("%Y%m%d")}.parquet'


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
    'stage_event_detail',
    default_args=default_args,
    description='A DAG to stage seoul event detail data every day and save it to S3',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
)

get_performance_detail_from_raw = PythonOperator(
    task_id='get_performance_detail_from_raw',
    python_callable=get_performance_detail_from_raw,
    dag=dag,
)

get_festival_detail_from_raw = PythonOperator(
    task_id='get_festival_detail_from_raw',
    python_callable=get_festival_detail_from_raw,
    dag=dag,
)

merge_tables = PythonOperator(
    task_id='merge_tables',
    python_callable=merge_tables,
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

get_performance_detail_from_raw >> get_festival_detail_from_raw >> merge_tables >> transform >> load_to_s3_stage >> copy_to_redshift