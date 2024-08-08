import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins import slack

import pytz
import io

import requests
import logging
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow.parquet as pq


API_SOURCE = 'kopis'
API_PERFORMANCE = 'performance'
API_PERF_DETAIL = 'performance_detail'
API_FESTIVAL = 'festival'
API_FEST_DETAIL = 'festival_detail'
API_FACILITY = 'facilities'
API_FACI_DETAIL = 'facility_detail'

logging.basicConfig(level=logging.INFO)



def load_raw_event_to_stage(**kwargs):
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    execution_date = kwargs['execution_date']

    # get performance, festival s3 raw key
    perf_s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_PERFORMANCE, API_PERFORMANCE, 'csv')
    fest_s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FESTIVAL, API_FESTIVAL, 'csv')

    # read csv files decoded to string
    s3 = S3Hook('s3_conn')
    perf_csv_string = s3.read_key(
        key=perf_s3_key,
        bucket_name=raw_bucket
    )
    fest_csv_string = s3.read_key(
        key=fest_s3_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to datafrmae
    perf_df = pd.read_csv(io.BytesIO(perf_csv_string.encode('utf-8')))
    fest_df = pd.read_csv(io.BytesIO(fest_csv_string.encode('utf-8')))

    # merge performance and festival dataframe
    event_df = pd.concat([perf_df, fest_df])
    event_df.reset_index(inplace=True)

    # transform dataframe match to stage, redshift table schema
    transformed_df = transform_event(event_df)
    add_timestamp_cols(transformed_df, convert_to_kst(execution_date))

    # convert dataframe to parquet bytes
    pq_bytes = convert_to_parquet_bytes(transformed_df)

    # define S3 path
    s3_key = get_s3_path(execution_date, API_SOURCE, 'event', 'event', 'parquet')

    # upload parquet file to s3
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    upload_byte_to_s3(s3, s3_key, pq_bytes, stage_bucket)

    return s3_key


def load_raw_event_detail_to_stage(**kwargs):
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    execution_date = kwargs['execution_date']

    # get performance, festival detail s3 raw key
    perf_s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_PERF_DETAIL, API_PERF_DETAIL, 'csv')
    fest_s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FEST_DETAIL, API_FEST_DETAIL, 'csv')

    # read csv files decoded to string
    s3 = S3Hook('s3_conn')
    perf_csv_string = s3.read_key(
        key=perf_s3_key,
        bucket_name=raw_bucket
    )
    fest_csv_string = s3.read_key(
        key=fest_s3_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to datafrmae
    perf_df = pd.read_csv(io.BytesIO(perf_csv_string.encode('utf-8')))
    fest_df = pd.read_csv(io.BytesIO(fest_csv_string.encode('utf-8')))

    # merge performance and festival dataframe
    event_df = pd.concat([perf_df, fest_df])
    event_df.reset_index(inplace=True)

    # transform dataframe match to stage, redshift table schema
    transformed_df = transform_event_detail(event_df)
    add_timestamp_cols(transformed_df, convert_to_kst(execution_date))

    # convert dataframe to parquet bytes
    pq_bytes = convert_to_parquet_bytes(transformed_df)

    # define S3 path
    s3_key = get_s3_path(execution_date, API_SOURCE, 'event_detail', 'event_detail', 'parquet')

    # upload parquet file to s3
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    upload_byte_to_s3(s3, s3_key, pq_bytes, stage_bucket)

    return s3_key


def load_raw_facilities_to_stage(**kwargs):
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    execution_date = kwargs['execution_date']

    # get performance, festival s3 raw key
    facilities_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FACILITY, API_FACILITY, 'csv')

    # read csv files decoded to string
    s3 = S3Hook('s3_conn')
    facilities_csv_string = s3.read_key(
        key=facilities_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to datafrmae
    facilities_df = pd.read_csv(io.BytesIO(facilities_csv_string.encode('utf-8')))

    # drop duplicate records using mt20id column
    unique_df = drop_duplicates(facilities_df, 'mt10id')

    # drop duplicated columns
    col_dropped_df = drop_column(
        unique_df,
        ['fcltynm', 'mt13cnt', 'fcltychartr', 'opende']
    )

    # add created and updated at timestamp columns
    add_timestamp_cols(col_dropped_df, convert_to_kst(execution_date))

    # convert dataframe to parquet bytes
    pq_bytes = convert_to_parquet_bytes(col_dropped_df)

    # define S3 path
    s3_key = get_s3_path(execution_date, API_SOURCE, API_FACILITY, API_FACILITY, 'parquet')

    # upload parquet file to s3
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    upload_byte_to_s3(s3, s3_key, pq_bytes, stage_bucket)

    return s3_key


def load_raw_facility_detail_to_stage(**kwargs):
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    execution_date = kwargs['execution_date']

    # get performance, festival s3 raw key
    detail_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FACI_DETAIL, API_FACI_DETAIL, 'csv')

    # read csv files decoded to string
    s3 = S3Hook('s3_conn')
    detail_csv_string = s3.read_key(
        key=detail_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to datafrmae
    detail_df = pd.read_csv(io.BytesIO(detail_csv_string.encode('utf-8')))

    # drop duplicate records using mt10id column
    unique_df = drop_duplicates(detail_df, 'mt10id')

    # change tel number to global tel number
    unique_df['telno'] = unique_df['telno'].apply(parse_to_global_tel)

    # add created and updated at timestamp columns
    add_timestamp_cols(unique_df, convert_to_kst(execution_date))

    # convert dataframe to parquet bytes
    pq_bytes = convert_to_parquet_bytes(unique_df)

    # define S3 path
    s3_key = get_s3_path(execution_date, API_SOURCE, API_FACI_DETAIL, API_FACI_DETAIL, 'parquet')

    # upload parquet file to s3
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    upload_byte_to_s3(s3, s3_key, pq_bytes, stage_bucket)

    return s3_key


def create_table_if_not_exists(**kwargs):
    # connect to redshift
    hook = connect_to_postgres(conn_id='redshift_conn', autocommit=False)
    cursor = hook.get_cursor()

    # create tables if not exists
    raw_schema = 'raw_data'
    event_table = 'event'
    create_event_sql = f"""CREATE TABLE IF NOT EXISTS {raw_schema}.{event_table} (
        mt20id varchar(256),
        festival varchar(1),
        createdAt timestamp default GETDATE(),
        updatedAt timestamp default GETDATE()
    );"""

    event_detail_table = 'event_detail'
    create_event_detail = f"""CREATE TABLE IF NOT EXISTS {raw_schema}.{event_detail_table} (
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
        createdAt timestamp default GETDATE(),
        updatedAt timestamp default GETDATE()
    );"""

    performace_facility_sidogu_table = 'performance_facility_sidogu'
    create_facility_sidogu_sql = f"""CREATE TABLE IF NOT EXISTS {raw_schema}.{performace_facility_sidogu_table} (
        mt10id varchar(256) primary key,
        sidonm varchar(256),
        gugunnm varchar(256),
        createdAt timestamp default GETDATE(),
        updatedAt timestamp default GETDATE()
    );
    """

    performance_facility_detail_table = 'performance_facility_detail'
    create_facility_detail_sql = f"""CREATE TABLE IF NOT EXISTS {raw_schema}.{performance_facility_detail_table} (
        fcltynm varchar(256),
        mt10id varchar(256) primary key,
        mt13cnt bigint,
        fcltychartr varchar(256),
        opende varchar(32),
        seatscale bigint,
        telno varchar(256),
        relateurl varchar(2048),
        adres varchar(2048),
        la double precision,
        lo double precision,
        createdAt timestamp default GETDATE(),
        updatedAt timestamp default GETDATE()
    );
    """

    try:
        cursor.execute(create_event_sql)
        cursor.execute(create_event_detail)
        cursor.execute(create_facility_sidogu_sql)
        cursor.execute(create_facility_detail_sql)
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        raise


def join_event_tables(**kwargs):
    ti = kwargs['ti']

    # get keys
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    event_s3_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_event_to_stage')
    event_detail_s3_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_event_detail_to_stage')

    # get event parquet file
    s3 = S3Hook('s3_conn')
    event_file_obj = s3.get_key(
        key=event_s3_key,
        bucket_name=stage_bucket
    )
    # get event_detail parquet file
    detail_file_obj = s3.get_key(
        key=event_detail_s3_key,
        bucket_name=stage_bucket
    )

    # read parquet files
    event_df = read_parquet_from_s3_obj(event_file_obj)
    detail_df = read_parquet_from_s3_obj(detail_file_obj)
    detail_df.drop(columns=['createdAt', 'updatedAt'], inplace=True)

    # join tables
    joined_df = pd.merge(event_df, detail_df, left_on='mt20id', right_on='mt20id', how='inner')

    # pick necessary columns
    result_df = joined_df[[
        'mt20id', 'prfnm', 'eventstart', 'eventend', 'seatprices', 'poster', 'genrenm', 'festival', 'createdAt', 'updatedAt',  'mt10id'
    ]]
    logging.info(result_df.columns)
    logging.info(result_df.dtypes)

    # define key to load to external zone
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    event_external_key = get_s3_path(kwargs['execution_date'], API_SOURCE, 'event', 'joined_event', 'parquet')

    # load to s3 external zone
    pq_bytes = convert_to_parquet_bytes(result_df)
    upload_byte_to_s3(s3, event_external_key, pq_bytes, external_bucket)

    return event_external_key


def join_facility_tables(**kwargs):
    """
    extract and merge tagbles: tour attractions info, tour area code info parquet files from s3 stage layer
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']

    # get keys
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    facilities_s3_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_facilities_to_stage')
    facility_detail_s3_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_facility_detail_to_stage')
    logging.info(facilities_s3_key)
    logging.info(facility_detail_s3_key)

    # get event parquet file
    s3 = S3Hook('s3_conn')
    facilities_file_obj = s3.get_key(
        key=facilities_s3_key,
        bucket_name=stage_bucket
    )

    # get event_detail parquet file
    detail_file_obj = s3.get_key(
        key=facility_detail_s3_key,
        bucket_name=stage_bucket
    )

    # read facilities parquet
    facilities_df = read_parquet_from_s3_obj(facilities_file_obj)

    # read area code parquet
    detail_df = read_parquet_from_s3_obj(detail_file_obj)
    detail_df.drop(columns=['createdAt', 'updatedAt'], inplace=True)

    # join tables
    joined_df = pd.merge(facilities_df, detail_df, left_on='mt10id', right_on='mt10id', how='inner')

    # pick necessary columns
    result_df = joined_df[[
        'mt10id', 'fcltynm', 'sidonm', 'gugunnm', 'adres', 'lo', 'la', 'createdAt', 'updatedAt'
    ]]
    result_df.rename(columns={'fcltynm': 'fclynm'}, inplace=True)

    # define key to load to temp zone
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    execution_date = kwargs['execution_date']
    s3_temp_key = get_s3_path(execution_date, API_SOURCE, API_FACILITY, 'joined_facilities', 'parquet')

    # load to s3 temp zone
    pq_bytes = convert_to_parquet_bytes(result_df)
    upload_byte_to_s3(s3, s3_temp_key, pq_bytes, external_bucket)

    return s3_temp_key


def ensure_event_integrity(**kwargs):
    """
    ensure data integrity before load table to rds
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')

    # get s3 key to extract joined table before task
    s3_temp_key = ti.xcom_pull(task_ids='produce_to_rds.join_event_tables')

    # get joined parquet file
    s3 = S3Hook('s3_conn')
    joined_file_obj = s3.get_key(
        key=s3_temp_key,
        bucket_name=external_bucket
    )

    # read parquet
    df = read_parquet_from_s3_obj(joined_file_obj)

    # drop duplicate by pk: contentid
    unique_df = drop_duplicates(df, 'mt20id')

    # manage NaN values
    raise_with_null_cols(unique_df, ['eventstart', 'eventend', 'createdAt', 'updatedAt'])
    unique_df = unique_df.fillna({'seatprices': 'NODATA', 'poster': 'NODATA', 'genrenm': 'NODATA', 'festival': 'N'})
    result_df = unique_df[['mt20id', 'prfnm', 'eventstart', 'eventend', 'seatprices', 'poster', 'genrenm', 'festival', 'createdAt',
             'updatedAt', 'mt10id']].dropna()

    # change format of eventStart, evnetEnd
    result_df['eventstart'] = result_df['eventstart'].astype('datetime64[ns]')
    result_df['eventend'] = result_df['eventend'].astype('datetime64[ns]')

    # rename columns
    result_df.rename(columns={
        'eventstart': 'eventStart',
        'eventend': 'eventEnd',
        'seatprices': 'seatPrice',
        'mt10id': 'mt10id_id'
    }, inplace=True)

    # define key to load to temp zone
    s3_ensured_key = get_s3_path(kwargs['execution_date'], API_SOURCE, 'event', 'ensured_event', 'parquet')

    # load to s3 temp zone
    pq_bytes = convert_to_parquet_bytes(result_df)
    upload_byte_to_s3(s3, s3_ensured_key, pq_bytes, external_bucket)

    # delete temp joined file in s3
    delete_file_in_s3(s3, s3_temp_key, external_bucket)

    return s3_ensured_key


def ensure_facility_integrity(**kwargs):
    """
    ensure data integrity before load table to rds
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')

    # get s3 key to extract joined table before task
    s3_temp_key = ti.xcom_pull(task_ids='produce_to_rds.join_facility_tables')

    # get joined parquet file
    s3 = S3Hook('s3_conn')
    joined_file_obj = s3.get_key(
        key=s3_temp_key,
        bucket_name=external_bucket
    )

    # read parquet
    df = read_parquet_from_s3_obj(joined_file_obj)

    # drop duplicate by pk: contentid
    unique_df = drop_duplicates(df, 'mt10id')

    # manage NaN values
    raise_with_null_cols(unique_df, ['createdAt', 'updatedAt'])
    unique_df = unique_df.fillna({'sidonm': '서울'})
    # Delete records including NULL with below columns
    result_df = unique_df[['mt10id', 'fclynm', 'sidonm', 'gugunnm', 'adres', 'lo', 'la', 'createdAt', 'updatedAt']].dropna()

    # define key to load to temp zone
    execution_date = kwargs['execution_date']
    s3_ensured_key = get_s3_path(execution_date, API_SOURCE, API_FACILITY, 'ensured_facilities', 'parquet')
    # load to s3 temp zone
    pq_bytes = convert_to_parquet_bytes(result_df)
    upload_byte_to_s3(s3, s3_ensured_key, pq_bytes, external_bucket)

    # delete temp joined file in s3
    delete_file_in_s3(s3, s3_temp_key, external_bucket)

    return s3_ensured_key



def load_event_to_rds(**kwargs):
    ti = kwargs['ti']
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    table = 'tour_event'
    schema = 'public'

    # get s3 key to extract joined table before task
    ensured_external_key = ti.xcom_pull(task_ids='produce_to_rds.ensure_event_integrity')
    # get joined parquet file
    s3 = S3Hook('s3_conn')
    s3_file_obj = s3.get_key(
        key=ensured_external_key,
        bucket_name=external_bucket
    )

    # read parquet
    df = read_parquet_from_s3_obj(s3_file_obj)

    # connect postgres rds
    hook = connect_to_postgres(conn_id='postgres_conn', autocommit=False)

    # update data to rd
    upsert_table_transaction(hook, schema, table, df, 'mt20id')

    # delete integrity ensured file in s3
    delete_file_in_s3(s3, ensured_external_key, external_bucket)


def load_facility_to_rds(**kwargs):
    ti = kwargs['ti']
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    table = 'tour_performancesfacilities'
    schema = 'public'

    # get s3 key to extract joined table before task
    ensured_external_key = ti.xcom_pull(task_ids='produce_to_rds.ensure_facility_integrity')
    # get joined parquet file
    s3 = S3Hook('s3_conn')
    s3_file_obj = s3.get_key(
        key=ensured_external_key,
        bucket_name=external_bucket
    )

    # read parquet
    df = read_parquet_from_s3_obj(s3_file_obj)

    # connect postgres rds
    hook = connect_to_postgres(conn_id='postgres_conn', autocommit=False)

    # update data to rd
    upsert_table_transaction(hook, schema, table, df, 'mt10id')

    # delete integrity ensured file in s3
    delete_file_in_s3(s3, ensured_external_key, external_bucket)


def transform_event(df):
    # drop duplicate records using mt20id column
    unique_df = drop_duplicates(df, 'mt20id')

    # drop duplicated columns
    col_dropped_df = drop_column(
        unique_df,
        ['index', 'prfnm', 'prfpdfrom', 'prfpdto', 'fcltynm', 'poster', 'genrenm', 'openrun', 'prfstate']
    )

    # change null value to 'N' in festival col
    col_dropped_df['festival'].fillna('N', inplace=True)

    return col_dropped_df


def transform_event_detail(df):
    # drop duplicate records using mt20id column
    unique_df = drop_duplicates(df, 'mt20id')
    # drop duplicated columns
    col_dropped_df = drop_column(unique_df, ['index'])
    # convert seat price to dict string
    col_dropped_df['seatprices'] = col_dropped_df['pcseguidance'].apply(convert_to_dict)
    # drop original seatprice column
    dropped_df = drop_column(col_dropped_df, 'pcseguidance')
    # convert time value
    convert_time_format(dropped_df, 'prfpdfrom', 'eventstart')
    convert_time_format(dropped_df, 'prfpdto', 'eventend')
    # drop original time column
    prfpdfrom_dropped = drop_column(dropped_df, 'prfpdfrom')
    prfpdto_dropped = drop_column(prfpdfrom_dropped, 'prfpdto')


    return prfpdto_dropped


def convert_to_dict(data):
    if isinstance(data, float) or data is None:
        return None
    logging.info(data)
    prices = data.split(', ')
    if prices[0].startswith('전석무료'):
        return "{'전석': '0원'}"
    price_dict = {}
    for info in prices:
        seat_info = info.split(' ')
        price_dict[seat_info[0]] = seat_info[1]
    return f'{price_dict}'


def convert_time_format(df, target_col, new_col):
    time_converted = df[target_col].apply(
        lambda date_str:
            datetime.strptime(str(date_str), '%Y.%m.%d')
                .strftime('%Y-%m-%d'),
    )
    logging.info(f'Time converted series: {time_converted}')
    df[new_col] = time_converted
    logging.info(f'After {target_col} col converted: {df.columns}')


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


def parse_to_global_tel(telno):
    if not telno.startswith('0'):
        return
    prefix = '+82-'
    global_tel = prefix + telno[1:]
    return global_tel


def raise_with_null_cols(df, cols):
    if isinstance(cols, str):
        cols = [cols]
    for col_name in cols:
        if len(df[df[col_name].isnull()]) > 0:
            raise ValueError(f"Null value has benn detected in '{col_name}' column")


def add_timestamp_cols(df, kst_date):
    createdAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    updateAt = datetime(kst_date.year, kst_date.month, kst_date.day, kst_date.hour, kst_date.minute, kst_date.second)
    df['createdAt'] = createdAt
    df['updatedAt'] = updateAt


def get_s3_path(execution_date, api_source, api_called, file_prefix, file_format):
    kst_date = convert_to_kst(execution_date)
    return (f"source/{api_source}/{api_called}"
            f"/{kst_date.year}/{kst_date.month}/{kst_date.day}"
            f"/{file_prefix}_{kst_date.strftime('%Y%m%d')}.{file_format}")


def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)


def convert_to_parquet_bytes(df):
    pq_buffer = io.BytesIO()
    df.to_parquet(pq_buffer, index=False)
    return pq_buffer.getvalue()


def convert_to_csv_string(df):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def read_parquet_from_s3_obj(s3_obj):
    byte_buffer = io.BytesIO(s3_obj.get()["Body"].read())
    logging.info(f"parquet schema: {pq.read_schema(byte_buffer)}")
    df = pd.read_parquet(byte_buffer)
    byte_buffer.close()
    return df


def upload_string_to_s3(s3, s3_key,csv_string, bucket):
    s3.load_string(
        string_data=csv_string,
        key=s3_key,
        bucket_name=bucket,
        replace=True
    )


def upload_byte_to_s3(s3, s3_key, bytes, bucket):
    s3.load_bytes(
        bytes_data=bytes,
        key=s3_key,
        bucket_name=bucket,
        replace=True
    )


def delete_file_in_s3(s3, s3_key, bucket):
    s3.delete_objects(
        keys=s3_key,
        bucket=bucket
    )


def connect_to_postgres(conn_id, autocommit=True):
    # connect postgres rds
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return hook


def upsert_table_transaction(hook, schema, table, df, pk):
    cursor = hook.get_cursor()
    try:
        hook.insert_rows(table=f"{schema}.{table}", rows=df.values.tolist(), target_fields=','.join(['"'+x+'"' for x in df.columns.to_list()]).split(','), replace=True, replace_index=f'"{pk}"')
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        raise


# Define default_args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack.on_failure_callback
}

# Define the DAG
dag = DAG(
    'etl_kopis_data',
    default_args=default_args,
    description='A DAG to update event list data every day and save it to S3, Redshift, RDS',
    schedule_interval='@once', # triggered by load kopis to raw dag every day
    start_date=datetime(2024, 7, 24),
    catchup=False,
    tags=['load-redshift', 'table:event', 'table:event_detail', 'table:performance_facility_detail', 'table:performance_facility_sidogu']
)


with TaskGroup("raw_to_stage", tooltip="Tasks for load raw data to stage bucket", dag=dag) as raw_to_stage:
    load_raw_event_to_stage = PythonOperator(
        task_id='load_raw_event_to_stage',
        python_callable=load_raw_event_to_stage,
        dag=dag
    )
    load_raw_event_detail_to_stage = PythonOperator(
        task_id='load_raw_event_detail_to_stage',
        python_callable=load_raw_event_detail_to_stage,
        dag=dag
    )
    load_raw_facilities_to_stage = PythonOperator(
        task_id='load_raw_facilities_to_stage',
        python_callable=load_raw_facilities_to_stage,
        dag=dag
    )
    load_raw_facility_detail_to_stage = PythonOperator(
        task_id='load_raw_facility_detail_to_stage',
        python_callable=load_raw_facility_detail_to_stage,
        dag=dag
    )

    [load_raw_event_to_stage, load_raw_event_detail_to_stage, load_raw_facilities_to_stage, load_raw_facility_detail_to_stage]


with TaskGroup("copy_to_redshift", tooltip="Tasks to copy stage data to redshift raw_data table", dag=dag) as copy_to_redshift:
    create_table_if_not_exists = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists,
        dag=dag
    )
    copy_to_redshift_raw_data_event = S3ToRedshiftOperator(
        task_id='copy_to_redshift_raw_data_event',
        s3_bucket=Variable.get('S3_STAGE_BUCKET'),
        s3_key="{{ task_instance.xcom_pull(task_ids='raw_to_stage.load_raw_event_to_stage') }}",
        schema='raw_data',
        table='event',
        copy_options=['parquet'],
        redshift_conn_id='redshift_conn',
        aws_conn_id='s3_conn',
        method='UPSERT',
        upsert_keys=['mt20id'],
    )
    copy_to_redshift_raw_data_event_detail = S3ToRedshiftOperator(
        task_id='copy_to_redshift_raw_data_event_detail',
        s3_bucket=Variable.get('S3_STAGE_BUCKET'),
        s3_key="{{ task_instance.xcom_pull(task_ids='raw_to_stage.load_raw_event_detail_to_stage') }}",
        schema='raw_data',
        table='event_detail',
        copy_options=['parquet'],
        redshift_conn_id='redshift_conn',
        aws_conn_id='s3_conn',
        method='UPSERT',
        upsert_keys=['mt20id'],
    )
    copy_to_redshift_raw_data_performance_facility_sidogu = S3ToRedshiftOperator(
        task_id='copy_to_redshift_raw_data_performance_facility_sidogu',
        s3_bucket=Variable.get('S3_STAGE_BUCKET'),
        s3_key="{{ task_instance.xcom_pull(task_ids='raw_to_stage.load_raw_facilities_to_stage') }}",
        schema='raw_data',
        table='performance_facility_sidogu',
        copy_options=['parquet'],
        redshift_conn_id='redshift_conn',
        aws_conn_id='s3_conn',
        method='UPSERT',
        upsert_keys=['mt10id'],
    )
    copy_to_redshift_raw_data_performance_facility_detail = S3ToRedshiftOperator(
        task_id='copy_to_redshift_raw_data_performance_facility_detail',
        s3_bucket=Variable.get('S3_STAGE_BUCKET'),
        s3_key="{{ task_instance.xcom_pull(task_ids='raw_to_stage.load_raw_facility_detail_to_stage') }}",
        schema='raw_data',
        table='performance_facility_detail',
        copy_options=['parquet'],
        redshift_conn_id='redshift_conn',
        aws_conn_id='s3_conn',
        method='UPSERT',
        upsert_keys=['mt10id'],
    )

    create_table_if_not_exists >> [copy_to_redshift_raw_data_event, copy_to_redshift_raw_data_event_detail, copy_to_redshift_raw_data_performance_facility_sidogu, copy_to_redshift_raw_data_performance_facility_detail]


with TaskGroup("produce_to_rds", tooltip="Tasks for load event data to rds event table", dag=dag) as produce_to_rds:
    join_event_tables = PythonOperator(
        task_id='join_event_tables',
        python_callable=join_event_tables,
        dag=dag
    )
    join_facility_tables = PythonOperator(
        task_id='join_facility_tables',
        python_callable=join_facility_tables,
        dag=dag
    )
    ensure_event_integrity = PythonOperator(
        task_id='ensure_event_integrity',
        python_callable=ensure_event_integrity,
        dag=dag
    )
    ensure_facility_integrity = PythonOperator(
        task_id='ensure_facility_integrity',
        python_callable=ensure_facility_integrity,
        dag=dag
    )
    load_event_to_rds = PythonOperator(
        task_id='load_event_to_rds',
        python_callable=load_event_to_rds,
        dag=dag
    )
    load_facility_to_rds = PythonOperator(
        task_id='load_facility_to_rds',
        python_callable=load_facility_to_rds,
        dag=dag
    )

    join_facility_tables >> ensure_facility_integrity >> load_facility_to_rds >> join_event_tables >> ensure_event_integrity >> load_event_to_rds

raw_to_stage >> copy_to_redshift >> produce_to_rds