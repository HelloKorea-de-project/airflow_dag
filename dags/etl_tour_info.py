from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from plugins import slack
import io
import requests
import logging
import json
import pytz
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta

from plugins.dbt_utils import create_dbt_task_group


API_SOURCE = 'tour'
API_CALLED = 'attractions'
FILE_PREFIX = 'tour_attractions'

logging.basicConfig(level=logging.INFO)


def load_tour_attractions_data_to_raw(**kwargs):
    api_url = 'http://apis.data.go.kr/B551011/EngService1/areaBasedList1'
    api_key = Variable.get('tour_api_key')
    api_key_decode = requests.utils.unquote(api_key)
    seoul_area_code = 1
    params = {
        'serviceKey': api_key_decode,  # 인증키, 필수 입력
        'MobileOS': 'ETC',  # 모바일 OS 구분, 필수 입력
        'MobileApp': 'Dev',  # 앱 이름, 필수 입력
        '_type': 'json', # json 형식으로 반환
        'numOfRows': 1000,  # 한 페이지 결과 수
        'areaCode': seoul_area_code,
    }

    fetched_df = fetch_all_pages(api_url, params)
    print(f'Fetched {len(fetched_df)} items')

    # convert dataframe to csv file encoded to st ring
    csv_string = convert_to_csv_string(fetched_df)

    # define path to load to raw bucket
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_CALLED, FILE_PREFIX, 'csv')
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_string_to_s3(s3, s3_key, csv_string, raw_bucket)

    return s3_key


def load_raw_attraction_data_to_stage(**kwargs):
    ti = kwargs['ti']
    raw_bucket = Variable.get('S3_RAW_BUCKET')

    # get target key path
    s3_key = ti.xcom_pull(task_ids='load_tour_attractions_data_to_raw')

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    csv_string = s3.read_key(
        key=s3_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to dataframe
    df = pd.read_csv(io.BytesIO(csv_string.encode('utf-8')))
    logging.info(f'{len(df)} records are read')
    logging.info(f'Columns: {df.columns}')

    # transform dataframe before load to stage
    transformed_df = transform_tour_attraction(df)

    # Convert DataFrame to parquet bytes
    pq_bytes = convert_to_parquet_bytes(transformed_df)

    # Define S3 path
    s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_CALLED, FILE_PREFIX, 'parquet')
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload parquet file to S3
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_byte_to_s3(s3, s3_key, pq_bytes, stage_bucket)

    return s3_key


def load_raw_area_code_to_stage(**kwargs):
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)

    # get target key path
    s3_key = 'source/tour/dimension/seoul_area_code.csv'

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    csv_string = s3.read_key(
        key=s3_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to dataframe
    df = pd.read_csv(io.BytesIO(csv_string.encode('utf-8')))
    logging.info(f'{len(df)} records are read')
    logging.info(f'Columns: {df.columns}')

    # add korName, createdAt, updatedAt col
    add_column_to_area_code(df, kst_date)

    # Convert DataFrame to parquet bytes
    pq_bytes = convert_to_parquet_bytes(df)

    # Define S3 path
    s3_key = 'source/tour/dimension/seoul_area_code.parquet'
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload parquet file to S3
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_byte_to_s3(s3, s3_key, pq_bytes, stage_bucket)

    return s3_key

def load_raw_service_code_to_stage(**kwargs):
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)

    # get target key path
    s3_key = 'source/tour/dimension/tour_service_code.csv'

    # read csv file decoded to string
    s3 = S3Hook('s3_conn')
    csv_string = s3.read_key(
        key=s3_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to dataframe
    df = pd.read_csv(io.BytesIO(csv_string.encode('utf-8')), header=1)
    logging.info(df)
    logging.info(f'{len(df)} records are read')
    logging.info(f'Columns: {df.columns}')

    # transform dataframe
    transformed_df = transform_service_code(df)
    add_timestamp_cols(transformed_df, kst_date)

    # Convert DataFrame to parquet bytes
    pq_bytes = convert_to_parquet_bytes(transformed_df)

    # Define S3 path
    s3_key = 'source/tour/dimension/tour_service_code.parquet'
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload parquet file to S3
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_byte_to_s3(s3, s3_key, pq_bytes, stage_bucket)

    return s3_key


def create_table_if_not_exists(**kwargs):
    # connect to redshift
    hook = connect_to_postgres(conn_id='redshift_conn', autocommit=False)
    cursor = hook.get_cursor()

    # create tables if not exists
    raw_schema = 'raw_data'
    seoul_tour_info_table = 'seoul_tour_info'
    create_seoul_tour_info_sql = f"""CREATE TABLE IF NOT EXISTS {raw_schema}.{seoul_tour_info_table} (
        addr1 varchar(256),
        addr2 varchar(256),
        cat1  varchar(32),
        cat2  varchar(32),
        cat3  varchar(32),
        contentid bigint primary key,
        contenttypeid bigint,
        firstimage varchar(256),
        firstimage2 varchar(256),
        cpyrhtDivcd varchar(32),
        mapx double precision,
        mapy double precision,
        mlevel double precision,
        sigungucode double precision,
        tel varchar(256),
        title varchar(256),
        zipcode varchar(32),
        createdAt timestamp,
        updatedAt timestamp  
    )"""

    dim_schema = 'dimension_data'
    seoul_area_code_table = 'seoul_area_code'
    create_seoul_area_code = f"""CREATE TABLE IF NOT EXISTS {dim_schema}.{seoul_area_code_table} (
        rnum bigint,
        code bigint primary key,
        name varchar(32),
        korName varchar(32),
        createdAt timestamp default GETDATE(),
        updatedAt timestamp default GETDATE()  
    )"""

    tour_service_code_table = 'tour_service_code'
    create_tour_service_code = f"""CREATE TABLE IF NOT EXISTS {dim_schema}.{tour_service_code_table} (
        contenttypeid bigint,
        contenttypename varchar(256),
        cat1 varchar(32),
        cat2 varchar(32),
        cat3 varchar(32) primary key,
        maincategory varchar(256),
        subcategory1 varchar(256),
        subcategory2 varchar(256),
        createdAt timestamp default GETDATE(),
        updatedAt timestamp default GETDATE()  
    )"""
    try:
        cursor.execute(create_seoul_tour_info_sql)
        cursor.execute(create_seoul_area_code)
        cursor.execute(create_tour_service_code)
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        raise

def join_stage_tables(**kwargs):
    ti = kwargs['ti']
    # get tour attraction stage key
    attr_stage_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_attraction_data_to_stage')
    logging.info(f"Attraction data path from stage bucket: {attr_stage_key}")
    # get area code stage key
    area_stage_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_area_code_to_stage')
    logging.info(f"Area code data path from stage bucket: {area_stage_key}")

    # get tour attraction stage file s3 object
    stage_bucket = Variable.get('S3_STAGE_BUCKET')
    s3 = S3Hook('s3_conn')
    attr_file_obj = s3.get_key(
        key=attr_stage_key,
        bucket_name=stage_bucket
    )
    # get area code parquet file s3 object
    area_file_obj = s3.get_key(
        key=area_stage_key,
        bucket_name=stage_bucket
    )

    # read parquet files from s3 objects
    attr_df = read_parquet_from_s3_obj(attr_file_obj)
    area_df = read_parquet_from_s3_obj(area_file_obj)

    # join tables
    attr_df.rename(columns={'sigungucode': 'code'}, inplace=True)
    area_df.drop(columns=['createdAt', 'updatedAt'], inplace=True)
    joined_df = pd.merge(attr_df, area_df, left_on='code', right_on='code', how='inner')

    # pick necessary columns
    joined_df = joined_df[[
        'contentid', 'addr1', 'title', 'firstimage', 'mapx', 'mapy', 'createdAt', 'updatedAt', 'code', 'cat3'
    ]]
    logging.info(joined_df.columns)
    logging.info(joined_df.dtypes)

    # define key to load to external zone
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    joined_external_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_CALLED, 'joined_tour_attractions', 'parquet')

    # load to s3 external zone
    pq_bytes = convert_to_parquet_bytes(joined_df)
    upload_byte_to_s3(s3, joined_external_key, pq_bytes, external_bucket)

    return joined_external_key


def ensure_attraction_integrity(**kwargs):
    """
    ensure data integrity before load table to rds
    :param kwargs:
    :return:
    """
    ti = kwargs['ti']

    # get s3 key to extract joined table before task
    joined_table_key = ti.xcom_pull(task_ids='join_stage_tables')
    logging.info(joined_table_key)

    # get joined parquet file
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    s3 = S3Hook('s3_conn')
    joined_file_obj = s3.get_key(
        key=joined_table_key,
        bucket_name=external_bucket
    )

    # read parquet
    df = read_parquet_from_s3_obj(joined_file_obj)

    # drop duplicate by pk: contentid
    unique_df = drop_duplicates(df, 'contentid')

    # manage NaN values
    null_managed_df = manage_attraction_null(unique_df)

    # change format 'code' column to int
    null_managed_df['code'] = null_managed_df['code'].astype('int')

    # remove records not contains 'Seoul' in addr col
    removed_df = null_managed_df[null_managed_df['addr1'].str.contains('Seoul')]

    # drop invalid code
    removed_df2 = removed_df[removed_df['cat3'].str.len() == 9]

    # rename columns to match with RDS table schema
    result_df = removed_df2.rename(
        columns={
            'contentid': 'contentID',
            'addr1': 'addr',
            'title': 'title',
            'firstimage': 'firstImage',
            'mapy': 'la',
            'mapx': 'lo',
            'createdAt': 'createdAt',
            'updatedAt': 'updatedAt',
            'code': 'siGunGuCode_id',
            'cat3': 'cat3_id',
        }
    )

    # define key to load to temp zone
    ensured_external_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_CALLED, 'ensured_tour_attractions', 'parquet')

    # load to s3 temp zone
    pq_bytes = convert_to_parquet_bytes(result_df)
    upload_byte_to_s3(s3, ensured_external_key, pq_bytes, external_bucket)

    # # delete temp joined file in s3
    delete_file_in_s3(s3, joined_table_key, external_bucket)

    return ensured_external_key


def ensure_area_integrity(**kwargs):
    ti = kwargs['ti']
    stage_bucket = Variable.get('S3_STAGE_BUCKET')

    # get s3 key to extract area code parquet file from stage bucket
    area_stage_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_area_code_to_stage')
    logging.info(area_stage_key)

    # get area code parquet file s3 object
    s3 = S3Hook('s3_conn')
    area_code_file_obj = s3.get_key(
        key=area_stage_key,
        bucket_name=stage_bucket
    )
    # read parquet
    df = read_parquet_from_s3_obj(area_code_file_obj)

    # pick only necessary columns
    df = df[['code', 'name', 'korName']]
    logging.info(f"Dataframe cols: {df.columns}")
    logging.info(f"Dataframe schema: {df.dtypes}")

    # drop duplicate by pk: contentid
    unique_df = drop_duplicates(df, 'code')

    # manage NaN values
    raise_with_null_cols(unique_df, ['code', 'name', 'korName'])

    # change format 'code' column to int
    unique_df['code'] = unique_df['code'].astype('int')

    # define key to load to external zone
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    ensured_external_key = f'source/tour/dimension/ensured_seoul_area_code.parquet'

    # load to s3 external zone
    pq_bytes = convert_to_parquet_bytes(unique_df)
    upload_byte_to_s3(s3, ensured_external_key, pq_bytes, external_bucket)

    return ensured_external_key


def ensure_service_code_integrity(**kwargs):
    ti = kwargs['ti']
    stage_bucket = Variable.get('S3_STAGE_BUCKET')

    # get s3 key to extract area code parquet file from stage bucket
    service_stage_key = ti.xcom_pull(task_ids='raw_to_stage.load_raw_service_code_to_stage')
    logging.info(service_stage_key)

    # get area code parquet file s3 object
    s3 = S3Hook('s3_conn')
    service_code_file_obj = s3.get_key(
        key=service_stage_key,
        bucket_name=stage_bucket
    )
    # read parquet
    df = read_parquet_from_s3_obj(service_code_file_obj)

    # pick only necessary columns
    df = df[
        ['cat3', 'cat2', 'cat1', 'contenttypename', 'contenttypeid', 'maincategory', 'subcategory1', 'subcategory2']]
    logging.info(f"Dataframe cols: {df.columns}")
    logging.info(f"Dataframe schema: {df.dtypes}")

    # drop duplicate by pk
    unique_df = drop_duplicates(df, 'cat3')

    # manage NaN values
    result_df = manage_service_code_null(unique_df)

    # change format 'code' column to int
    result_df['contenttypeid'] = result_df['contenttypeid'].astype('int')

    # rename cols
    result_df.rename(columns={
        'contenttypename': 'contentTypeName',
        'contenttypeid': 'contentTypeID',
        'maincategory': 'mainCategory',
        'subcategory1': 'subCategory1',
        'subcategory2': 'subCategory2'
    }, inplace=True)

    # define key to load to external zone
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    ensured_external_key = f'source/tour/dimension/ensured_service_code.parquet'

    # load to s3 external zone
    pq_bytes = convert_to_parquet_bytes(result_df)
    upload_byte_to_s3(s3, ensured_external_key, pq_bytes, external_bucket)

    return ensured_external_key


def load_attraction_to_rds(**kwargs):
    ti = kwargs['ti']
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    table = 'tour_seoultourinfo'
    schema = 'public'
    # get data integrity ensured file key
    ensured_external_key = ti.xcom_pull(task_ids='ensure_integrity.ensure_attraction_integrity')
    logging.info(ensured_external_key)
    # get file obj
    s3 = S3Hook('s3_conn')
    s3_obj = s3.get_key(
        key=ensured_external_key,
        bucket_name=external_bucket
    )

    # read parquet file
    df = read_parquet_from_s3_obj(s3_obj)

    # connect postgres rds
    hook = connect_to_postgres(conn_id='postgres_conn', autocommit=False)

    # update data to rds
    upsert_table_transaction(hook, schema, table, df, 'contentID')

    # delete integrity ensured file in s3
    delete_file_in_s3(s3, ensured_external_key, external_bucket)


def load_area_to_rds(**kwargs):
    ti = kwargs['ti']
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    table = 'tour_seoulareacode'
    schema = 'public'
    # get data integrity ensured file key
    ensured_external_key = ti.xcom_pull(task_ids='ensure_integrity.ensure_area_integrity')
    logging.info(ensured_external_key)
    # get file obj
    s3 = S3Hook('s3_conn')
    s3_obj = s3.get_key(
        key=ensured_external_key,
        bucket_name=external_bucket
    )

    # read parquet file
    df = read_parquet_from_s3_obj(s3_obj)

    # connect postgres rds
    hook = connect_to_postgres(conn_id='postgres_conn', autocommit=False)

    # update data to rds
    upsert_table_transaction(hook, schema, table, df, 'code')

    # delete integrity ensured file in s3
    delete_file_in_s3(s3, ensured_external_key, external_bucket)


def load_service_code_to_rds(**kwargs):
    ti = kwargs['ti']
    external_bucket = Variable.get('S3_EXTERNAL_BUCKET')
    table = 'tour_tourismservicecategory'
    schema = 'public'
    # get data integrity ensured file key
    ensured_external_key = ti.xcom_pull(task_ids='ensure_integrity.ensure_service_code_integrity')
    logging.info(ensured_external_key)
    # get file obj
    s3 = S3Hook('s3_conn')
    s3_obj = s3.get_key(
        key=ensured_external_key,
        bucket_name=external_bucket
    )

    # read parquet file
    df = read_parquet_from_s3_obj(s3_obj)

    # connect postgres rds
    hook = connect_to_postgres(conn_id='postgres_conn', autocommit=False)

    # update data to rds
    upsert_table_transaction(hook, schema, table, df, 'cat3')

    # delete integrity ensured file in s3
    delete_file_in_s3(s3, ensured_external_key, external_bucket)


def fetch_page(api_url, params=None, headers=None):
    try:
        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logging.error(f'Other error occurred: {err}')
    return None


def fetch_all_pages(api_url, params=None):
    all_data = []
    page = 1

    while True:
        params['pageNo'] = page
        logging.info(f'Fetching page {page}')
        page_data = fetch_page(api_url, params)
        page_json = json.loads(page_data)
        if not page_data or page_json['response']['body']['numOfRows'] == 0:
            break
        all_data.extend(page_json['response']['body']['items']['item'])

        page += 1

    df = pd.DataFrame(all_data)
    return df


def transform_tour_attraction(df):
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

    return modifiedtime_dropped


def add_column_to_area_code(df, kst_date):
    # add korName col
    df['korName'] = ['강남구', '강동구', '강북구', '강서구', '관악구', '광진구', '구로구', '금천구', '노원구', '도봉구',
                     '동대문구', '동작구', '마포구', '서대문구', '서초구', '성동구', '성북구', '송파구', '양천구', '영등포구',
                     '용산구', '은평구', '종로구', '중구', '중랑구']

    # add created and updated at timestamp columns
    add_timestamp_cols(df, kst_date)
    logging.info(df)
    logging.info(df.columns, df.dtypes)


def transform_service_code(df):
    # parse contenttype id to int and created new column 'contenttypename'
    new_id_sr = df['contenttypeid'].apply(lambda data: int(data.split('\n')[0].split(' ')[1][1:-1]))
    new_name_sr = df['contenttypeid'].apply(lambda data: ''.join(data.split('\n')[1:]).replace(' ', ''))
    df['contenttypeid'] = new_id_sr
    df['contenttypename'] = new_name_sr

    # pick necessary columms
    dropped_df = df[[
        'contenttypeid', 'contenttypename',
        '대분류 (cat1)', '중분류 (cat2)', '소분류 (cat3)',
        '대분류(Main category)', '중분류(Sub category 1)', '소분류(Sub category 2)'
    ]]

    # rename columns
    renamed_df = dropped_df.rename(columns={
        '대분류 (cat1)': 'cat1',
        '중분류 (cat2)': 'cat2',
        '소분류 (cat3)': 'cat3',
        '대분류(Main category)': 'maincategory',
        '중분류(Sub category 1)': 'subcategory1',
        '소분류(Sub category 2)': 'subcategory2',
    })

    return renamed_df

def manage_attraction_null(df):
    # Fill firstimage Null value with 'NODATA' string
    df = df.fillna({'firstimage': 'NODATA'})
    # Raise exception when code is Null
    raise_with_null_cols(df, ['code', 'createdAt', 'updatedAt'])
    # Delete records including NULL with below columns
    df = df[['contentid', 'addr1', 'title', 'firstimage', 'mapx', 'mapy', 'createdAt', 'updatedAt', 'code', 'cat3']].dropna()

    return df


def manage_service_code_null(df):
    # Raise exception with Null
    raise_with_null_cols(df, ['cat3', 'contenttypeid', 'contenttypename', 'maincategory', 'subcategory1', 'subcategory2'])
    # fill cat1, cat2 with cat3
    df = df.fillna({'cat1': df['cat3'].str[:3], 'cat2': df['cat3'].str[:5]})

    return df


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


def convert_to_parquet_bytes(df):
    pq_buffer = io.BytesIO()
    df.to_parquet(pq_buffer, engine='pyarrow', use_deprecated_int96_timestamps=True, index=False)
    logging.info(pq.read_schema(pq_buffer))
    return pq_buffer.getvalue()


def convert_to_csv_string(df):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def convert_time_format(df, target_col, new_col):
    time_converted = df[target_col].apply(lambda date_str: datetime.strptime(str(date_str), '%Y%m%d%H%M%S'))
    logging.info(f'Time converted series: {time_converted}')
    df[new_col] = time_converted
    logging.info(f'After {target_col} col converted: {df.columns}')


def get_s3_path(execution_date, api_source, api_called, file_prefix, file_format):
    kst_date = convert_to_kst(execution_date)
    return (f"source/{api_source}/{api_called}"
            f"/{kst_date.year}/{kst_date.month}/{kst_date.day}"
            f"/{file_prefix}_{kst_date.strftime('%Y%m%d')}.{file_format}")


def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)



def upload_string_to_s3(s3, s3_key, csv_string, bucket):
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

def read_parquet_from_s3_obj(s3_obj):
    byte_buffer = io.BytesIO(s3_obj.get()["Body"].read())
    logging.info(f"parquet schema: {pq.read_schema(byte_buffer)}")
    df = pd.read_parquet(byte_buffer)
    byte_buffer.close()
    return df


# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack.on_failure_callback
}

dag = DAG(
    'etl_tour_attractions_data',
    default_args=default_args,
    description='A DAG to update tour attractions data every day and load it to S3, redshift and rds',
    schedule_interval='0 4 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['load-redshift', 'table:seoul_tour_info']
)

load_tour_attractions_data_to_raw = PythonOperator(
        task_id='load_tour_attractions_data_to_raw',
        python_callable=load_tour_attractions_data_to_raw,
        dag=dag,
    )

with TaskGroup("raw_to_stage", tooltip="Tasks for load raw bucket data to stage bucket", dag=dag) as raw_to_stage:
    load_raw_attraction_data_to_stage = PythonOperator(
        task_id='load_raw_attraction_data_to_stage',
        python_callable=load_raw_attraction_data_to_stage,
    )
    load_raw_area_code_to_stage = PythonOperator(
        task_id='load_raw_area_code_to_stage',
        python_callable=load_raw_area_code_to_stage,
    )
    load_raw_service_code_to_stage = PythonOperator(
        task_id='load_raw_service_code_to_stage',
        python_callable=load_raw_service_code_to_stage,
    )

    [load_raw_attraction_data_to_stage, load_raw_area_code_to_stage, load_raw_service_code_to_stage]


with TaskGroup("copy_to_redshift", tooltip="Tasks for copy stage bucket files to redshift raw_data schema", dag=dag) as copy_to_redshift:
    create_table_if_not_exists = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists,
        dag=dag
    )

    copy_to_redshift_raw_data_seoultourinfo = S3ToRedshiftOperator(
        task_id='copy_to_redshift_raw_data_seoultourinfo',
        s3_bucket=Variable.get('S3_STAGE_BUCKET'),
        s3_key="{{ task_instance.xcom_pull(task_ids='raw_to_stage.load_raw_attraction_data_to_stage') }}",
        schema='raw_data',
        table='seoul_tour_info',
        copy_options=['parquet'],
        redshift_conn_id='redshift_conn',
        aws_conn_id='s3_conn',
        method='UPSERT',
        upsert_keys=['contentid'],
    )
    copy_to_redshift_dimension_data_seoulareacode = S3ToRedshiftOperator(
        task_id='copy_to_redshift_dimension_data_seoulareacode',
        s3_bucket=Variable.get('S3_STAGE_BUCKET'),
        s3_key="{{ task_instance.xcom_pull(task_ids='raw_to_stage.load_raw_area_code_to_stage') }}",
        schema='dimension_data',
        table='seoul_area_code',
        copy_options=['parquet'],
        redshift_conn_id='redshift_conn',
        aws_conn_id='s3_conn',
        method='UPSERT',
        upsert_keys=['code'],
    )
    copy_to_redshift_dimension_data_tourservicecode = S3ToRedshiftOperator(
        task_id='copy_to_redshift_dimension_data_tourservicecode',
        s3_bucket=Variable.get('S3_STAGE_BUCKET'),
        s3_key="{{ task_instance.xcom_pull(task_ids='raw_to_stage.load_raw_service_code_to_stage') }}",
        schema='dimension_data',
        table='tour_service_code',
        copy_options=['parquet'],
        redshift_conn_id='redshift_conn',
        aws_conn_id='s3_conn',
        method='UPSERT',
        upsert_keys=['cat3'],
    )

    create_table_if_not_exists >> [copy_to_redshift_raw_data_seoultourinfo, copy_to_redshift_dimension_data_seoulareacode, copy_to_redshift_dimension_data_tourservicecode]

join_stage_tables = PythonOperator(
    task_id='join_stage_tables',
    python_callable=join_stage_tables,
    dag=dag
)

with TaskGroup("ensure_integrity", tooltip='Tasks for ensure data integrity', dag=dag) as ensure_integrity:
    ensure_attraction_integrity = PythonOperator(
        task_id='ensure_attraction_integrity',
        python_callable=ensure_attraction_integrity,
    )
    ensure_area_integrity = PythonOperator(
        task_id='ensure_area_integrity',
        python_callable=ensure_area_integrity
    )
    ensure_service_code_integrity = PythonOperator(
        task_id='ensure_service_code_integrity',
        python_callable=ensure_service_code_integrity
    )
    [ensure_attraction_integrity, ensure_area_integrity, ensure_service_code_integrity]


with TaskGroup("produce_to_rds", tooltip="Tasks for produce integrity ensured data to rds", dag=dag) as produce_to_rds:
    load_area_to_rds = PythonOperator(
        task_id='load_area_to_rds',
        python_callable=load_area_to_rds,
        dag=dag
    )
    load_service_code_to_rds = PythonOperator(
        task_id='load_service_code_to_rds',
        python_callable=load_service_code_to_rds,
        dag=dag
    )
    load_attraction_to_rds = PythonOperator(
        task_id='load_attraction_to_rds',
        python_callable=load_attraction_to_rds,
        dag=dag
    )

    [load_area_to_rds, load_service_code_to_rds] >> load_attraction_to_rds


dbt_source_test_task_group = create_dbt_task_group(
    group_id="dbt_source_test_task_group",
    select_models=['fresh_st_info'],
    dag=dag
)

load_tour_attractions_data_to_raw >> raw_to_stage >> copy_to_redshift >> join_stage_tables >> ensure_integrity >> produce_to_rds >> dbt_source_test_task_group
