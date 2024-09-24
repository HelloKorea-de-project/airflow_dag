import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.models import Variable

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


def load_performance_data_to_raw(**kwargs):
    api_url = 'https://www.kopis.or.kr/openApi/restful/pblprfr'
    performance_api_key = Variable.get('kopis_api_key_performance')
    api_key_decode = requests.utils.unquote(performance_api_key)
    seoul_area_code = 11
    stdate = (kwargs['execution_date'] + timedelta(days=1)).strftime("%Y%m%d")
    eddate = (kwargs['execution_date'] + timedelta(days=30)).strftime("%Y%m%d")
    logging.info(f'{stdate} to {eddate}')
    params = {
        'service': api_key_decode,
        'stdate': stdate,
        'eddate': eddate,
        'rows': 1000,
        'signgucode': seoul_area_code,
    }

    # extract performance list data
    fetched_df = fetch_all_lists(api_url, API_PERFORMANCE,params)
    print(f'Fetched {len(fetched_df)} items')

    # convert dataframe to csf file encoded to string
    csv_string = convert_to_csv_string(fetched_df)

    # define path to load to raw bucket
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_PERFORMANCE, API_PERFORMANCE, 'csv')
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_string_to_s3(s3, s3_key, csv_string, raw_bucket)

    return s3_key


def load_festival_data_to_raw(**kwargs):
    api_url = 'http://kopis.or.kr/openApi/restful/prffest'
    api_key = Variable.get('kopis_api_key_facility')
    api_key_decode = requests.utils.unquote(api_key)
    seoul_area_code = 11
    stdate = (kwargs['execution_date'] + timedelta(days=1)).strftime("%Y%m%d")
    eddate = (kwargs['execution_date'] + timedelta(days=30)).strftime("%Y%m%d")
    logging.info(f'{stdate} to {eddate}')
    params = {
        'service': api_key_decode,
        'stdate': stdate,
        'eddate': eddate,
        'rows': 1000,
        'signgucode': seoul_area_code,
    }

    fetched_df = fetch_all_lists(api_url, API_FESTIVAL, params)
    print(f'Fetched {len(fetched_df)} items')

    # convert dataframe to csf file encoded to string
    csv_string = convert_to_csv_string(fetched_df)

    # define path to load to raw bucket
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FESTIVAL, API_FESTIVAL, 'csv')
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_string_to_s3(s3, s3_key, csv_string, raw_bucket)

    return s3_key


def load_detail_event_data_to_raw(**kwargs):
    api_url = 'https://www.kopis.or.kr/openApi/restful/pblprfr'
    performance_api_key = Variable.get('kopis_api_key_performance')
    api_key_decode = requests.utils.unquote(performance_api_key)
    params = {
        'service': api_key_decode
    }

    # get performance, festival list data s3 key
    performance_list_key = kwargs['ti'].xcom_pull(task_ids='load_list_data_to_raw.load_performance_data_to_raw')
    festival_list_key = kwargs['ti'].xcom_pull(task_ids='load_list_data_to_raw.load_festival_data_to_raw')

    # read csv file decoded to string
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    s3 = S3Hook('s3_conn')
    perf_csv_string = s3.read_key(
        key=performance_list_key,
        bucket_name=raw_bucket
    )
    fest_csv_string = s3.read_key(
        key=festival_list_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to dataframe
    perf_df = pd.read_csv(io.BytesIO(perf_csv_string.encode('utf-8')))
    logging.info(f'{len(perf_df)} records are read')
    logging.info(f'Columns: {perf_df.columns}')
    fest_df = pd.read_csv(io.BytesIO(fest_csv_string.encode('utf-8')))
    logging.info(f'{len(fest_df)} records are read')
    logging.info(f'Columns: {fest_df.columns}')

    # extract performance detail data
    perf_fetched_df = fetch_all_details(api_url, perf_df['mt20id'], API_PERF_DETAIL, params)
    print(f'Fetched {len(perf_fetched_df)} items')
    fest_fetched_df = fetch_all_details(api_url, fest_df['mt20id'], API_FEST_DETAIL, params)
    print(f'Fetched {len(fest_fetched_df)} items')


    # convert dataframe to csv file encoded to string
    perf_csv_string = convert_to_csv_string(perf_fetched_df)
    fest_csv_string = convert_to_csv_string(fest_fetched_df)

    # define path to load to raw bucket
    perf_s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_PERF_DETAIL, API_PERF_DETAIL, 'csv')
    logging.info(f's3_key will be loaded: {perf_s3_key}')
    fest_s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FEST_DETAIL, API_FEST_DETAIL, 'csv')
    logging.info(f's3_key will be loaded: {perf_s3_key}')

    # Upload to S3
    upload_string_to_s3(s3, perf_s3_key, perf_csv_string, raw_bucket)
    upload_string_to_s3(s3, fest_s3_key, fest_csv_string, raw_bucket)

    # xcom push keys
    kwargs['ti'].xcom_push(key='performance_detail_s3_raw_key', value=perf_s3_key)
    kwargs['ti'].xcom_push(key='festival_detail_s3_raw_key', value=fest_s3_key)


def load_facilities_data_to_raw(**kwargs):
    api_url = 'http://kopis.or.kr/openApi/restful/prfplc'
    api_key = Variable.get('kopis_api_key_facility')
    api_key_decode = requests.utils.unquote(api_key)
    seoul_area_code = 11
    params = {
        'service': api_key_decode,
        'rows': 1000,
        'signgucode': seoul_area_code,
    }

    fetched_df = fetch_all_lists(api_url, API_FACILITY, params)
    print(f'Fetched {len(fetched_df)} items')

    # convert dataframe to csv file encoded to string
    csv_string = convert_to_csv_string(fetched_df)

    # define path to load to raw bucket
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FACILITY, API_FACILITY, 'csv')
    logging.info(f's3_key will be loaded: {s3_key}')

    # upload to s3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_string_to_s3(s3, s3_key, csv_string, raw_bucket)

    return s3_key


def load_facility_detail_data_to_raw(**kwargs):
    api_url = 'http://kopis.or.kr/openApi/restful/prfplc'
    api_key = Variable.get('kopis_api_key_facility')
    api_key_decode = requests.utils.unquote(api_key)
    params = {
        'service': api_key_decode
    }

    # get facility list data s3 key
    facilities_key = kwargs['ti'].xcom_pull(task_ids='load_list_data_to_raw.load_facilities_data_to_raw')

    # read csv file decoded to string
    raw_bucket = Variable.get('S3_RAW_BUCKET')
    s3 = S3Hook('s3_conn')
    facilities_csv_string = s3.read_key(
        key=facilities_key,
        bucket_name=raw_bucket
    )

    # convert csv-string to dataframe
    facilities_df = pd.read_csv(io.BytesIO(facilities_csv_string.encode('utf-8')))
    logging.info(f'{len(facilities_df)} records are read')
    logging.info(f'Columns: {facilities_df.columns}')

    # extract performance detail data
    fetched_df = fetch_all_details(api_url, facilities_df['mt10id'], API_FACI_DETAIL, params)
    print(f'Fetched {len(fetched_df)} items')

    # convert dataframe to csv file encoded to string
    detail_csv_string = convert_to_csv_string(fetched_df)

    # define path to load to raw bucket
    detail_s3_key = get_s3_path(kwargs['execution_date'], API_SOURCE, API_FACI_DETAIL, API_FACI_DETAIL, 'csv')
    logging.info(f's3_key will be loaded: {detail_s3_key}')

    # upload to s3
    upload_string_to_s3(s3, detail_s3_key, detail_csv_string, raw_bucket)

    return detail_s3_key


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


def fetch_all_lists(api_url, event_type, params=None):
    all_data = []
    page = 1

    while True:
        params['cpage'] = page
        logging.info(f'Fetching page {page}')
        page_data = fetch_page(api_url, params)

        if not page_data or '<db>' not in page_data:
            break
        if event_type == API_PERFORMANCE:
            all_data.extend(parse_xml_performances(page_data))
        elif event_type == API_FESTIVAL:
            all_data.extend(parse_xml_festivals(page_data))
        elif event_type == API_FACILITY:
            all_data.extend(parse_xml_facilities(page_data))
        else:
            raise AttributeError("Invalid type input")

        page += 1

    df = pd.DataFrame(all_data)

    return df


def fetch_all_details(api_url, perf_ids, type, params=None):
    all_data = []

    for idx, perf_id in enumerate(perf_ids, start=1):
        detail_api_url = api_url + f'/{perf_id}'
        logging.info(f'Fetching {idx}th item {perf_id}')
        page_data = fetch_page(detail_api_url, params)

        if not page_data or '<db>' not in page_data:
            break
        if type in (API_PERF_DETAIL, API_FEST_DETAIL):
            all_data.extend(parse_xml_event_details(page_data))
        elif type == API_FACI_DETAIL:
            all_data.extend(parse_xml_facility_details(page_data))
        else:
            raise AttributeError("Invalid type input")
        time.sleep(0.5)

    df = pd.DataFrame(all_data)

    return df


def parse_xml_performances(xml_data):
    xml_root = ET.fromstring(xml_data)

    # XML 데이터를 순회하며 필요한 데이터 추출
    all_data = []
    for db in xml_root.findall('db'):
        row = {
            'mt20id': db.find('mt20id').text,
            'prfnm': db.find('prfnm').text,
            'prfpdfrom': db.find('prfpdfrom').text,
            'prfpdto': db.find('prfpdto').text,
            'fcltynm': db.find('fcltynm').text,
            'poster': db.find('poster').text,
            'genrenm': db.find('genrenm').text,
            'openrun': db.find('openrun').text,
            'prfstate': db.find('prfstate').text,
        }
        all_data.append(row)
    return all_data


def parse_xml_event_details(xml_data):
    xml_root = ET.fromstring(xml_data)

    # XML 데이터를 순회하며 필요한 데이터 추출
    all_data = []
    for db in xml_root.findall('db'):
        row = {
            'mt20id': db.find('mt20id').text, # 공연 ID
            'prfnm': db.find('prfnm').text, # 공연명
            'prfpdfrom': db.find('prfpdfrom').text, # 공연시작일
            'prfpdto': db.find('prfpdto').text, # 공연종료일
            'fcltynm': db.find('fcltynm').text, # 공연시설명(공연장명)
            'prfcast': db.find('prfcast').text, # 공연출연진
            'prfcrew': db.find('prfcrew').text, # 공연제작진
            'prfruntime': db.find('prfruntime').text, # 공연런타임
            'prfage': db.find('prfage').text, # 공연 관람 연령
            'entrpsnm': db.find('entrpsnm').text, #가획제작사
            'pcseguidance': db.find('pcseguidance').text, # 티켓가격
            'poster': db.find('poster').text, # 포스터 이미지 url
            'sty': db.find('sty').text, # 줄거리
            'genrenm': db.find('genrenm').text, # 장르
            'openrun': db.find('openrun').text, # 오픈런 여부
            'prfstate': db.find('prfstate').text, # 공연상태
            'mt10id': db.find('mt10id').text, # 공연시설 ID
            'dtguidance': db.find('dtguidance').text, # 공연시간
        }
        all_data.append(row)
    return all_data


def parse_xml_festivals(xml_data):
    xml_root = ET.fromstring(xml_data)

    # XML 데이터를 순회하며 필요한 데이터 추출
    all_data = []
    for db in xml_root.findall('db'):
        row = {
            'mt20id': db.find('mt20id').text,
            'prfnm': db.find('prfnm').text,
            'prfpdfrom': db.find('prfpdfrom').text,
            'prfpdto': db.find('prfpdto').text,
            'fcltynm': db.find('fcltynm').text,
            'poster': db.find('poster').text,
            'genrenm': db.find('genrenm').text,
            'prfstate': db.find('prfstate').text,
            'festival' : db.find('festival').text
        }
        all_data.append(row)
    return all_data


def parse_xml_facilities(xml_data):
    xml_root = ET.fromstring(xml_data)

    # XML 데이터를 순회하며 필요한 데이터 추출
    all_data = []
    for db in xml_root.findall('db'):
        row = {
            'fcltynm': db.find('fcltynm').text,
            'mt10id': db.find('mt10id').text,
            'mt13cnt': db.find('mt13cnt').text,
            'fcltychartr': db.find('fcltychartr').text,
            'sidonm': db.find('sidonm').text,
            'gugunnm': db.find('gugunnm').text,
            'opende': db.find('opende').text,
        }
        all_data.append(row)
    return all_data


def parse_xml_facility_details(xml_data):
    xml_root = ET.fromstring(xml_data)

    # XML 데이터를 순회하며 필요한 데이터 추출
    all_data = []
    for db in xml_root.findall('db'):
        row = {
            'fcltynm': db.find('fcltynm').text, # 시설명
            'mt10id': db.find('mt10id').text, # 공연 시설 ID
            'mt13cnt': db.find('mt13cnt').text, # 공연장 수
            'fcltychartr': db.find('fcltychartr').text, # 시설특성
            'opende': db.find('opende').text, # 개관연도
            'seatscale': db.find('seatscale').text, #객석 수
            'telno': db.find('telno').text,# 전화번호
            'relateurl': db.find('relateurl').text, # 홈페이지
            'adres': db.find('adres').text, # 주소
            'la': db.find('la').text, # 위도
            'lo': db.find('lo').text, # 경도
        }
        all_data.append(row)
    return all_data


def convert_to_csv_string(df):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()
    csv_buffer.close()
    return csv_string


def get_s3_path(execution_date, api_source, api_called, file_prefix, file_format):
    kst_date = convert_to_kst(execution_date)
    return (f"source/{api_source}/{api_called}"
            f"/{kst_date.year}/{kst_date.month}/{kst_date.day}"
            f"/{file_prefix}_{kst_date.strftime('%Y%m%d')}.{file_format}")


def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)


def upload_string_to_s3(s3, s3_key,csv_string, bucket):
    s3.load_string(
        string_data=csv_string,
        key=s3_key,
        bucket_name=bucket,
        replace=True
    )

# Define default_args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'load_kopis_to_raw',
    default_args=default_args,
    description='A DAG to update event list data every day and save it to S3 raw bucket',
    schedule_interval='0 4 * * *',
    start_date=datetime(2024, 7, 24),
    catchup=False,
)


with TaskGroup("load_list_data_to_raw", tooltip="Tasks for load kopis list data to s3 raw bucket", dag=dag) as load_list_data_to_raw:
    load_performance_data_to_raw = PythonOperator(
        task_id='load_performance_data_to_raw',
        python_callable=load_performance_data_to_raw,
        dag=dag
    )
    load_festival_data_to_raw = PythonOperator(
        task_id='load_festival_data_to_raw',
        python_callable=load_festival_data_to_raw,
        dag=dag
    )
    load_facilities_data_to_raw = PythonOperator(
        task_id='load_facilities_data_to_raw',
        python_callable=load_facilities_data_to_raw,
        dag=dag
    )

    [load_performance_data_to_raw, load_festival_data_to_raw, load_facilities_data_to_raw]


with TaskGroup("load_detail_data_to_raw", tooltip="Tasks for load kopis detail data to s3 raw bucket", dag=dag) as load_detail_data_to_raw:
    load_detail_event_data_to_raw = PythonOperator(
        task_id='load_detail_event_data_to_raw',
        python_callable=load_detail_event_data_to_raw,
        dag=dag
    )
    load_facility_detail_data_to_raw = PythonOperator(
        task_id='load_facility_detail_data_to_raw',
        python_callable=load_facility_detail_data_to_raw,
        dag=dag
    )

    [load_detail_event_data_to_raw, load_facility_detail_data_to_raw]

trigger_etl_kopis_data = TriggerDagRunOperator(
    task_id='trigger_etl_kopis_data',
    trigger_dag_id='etl_kopis_data',
    execution_date='{{ ds }}',
    reset_dag_run=True,
    dag=dag
)


load_list_data_to_raw >> load_detail_data_to_raw >> trigger_etl_kopis_data
