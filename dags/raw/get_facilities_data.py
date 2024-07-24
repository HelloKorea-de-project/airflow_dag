import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable

import pytz
import io

import requests
import logging
import xml.etree.ElementTree as ET
import pandas as pd

# 로깅 설정
logging.basicConfig(level=logging.INFO)

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


def fetch_all_facilities(api_url, params=None):
    all_data = []
    page = 1

    while True:
        params['cpage'] = page
        logging.info(f'Fetching page {page}')
        page_data = fetch_page(api_url, params)

        if not page_data or '<db>' not in page_data:
            break

        all_data.extend(parse_xml_facilities(page_data))

        page += 1

    df = pd.DataFrame(all_data)

    return df


def fetch_all_facility_details(api_url, fclt_ids, params=None):
    all_data = []

    for idx, fclt_id in enumerate(fclt_ids, start=1):
        detail_api_url = api_url + f'/{fclt_id}'
        logging.info(f'Fetching {idx}th item {fclt_id}')
        page_data = fetch_page(detail_api_url, params)

        if not page_data or '<db>' not in page_data:
            break

        all_data.extend(parse_xml_facility_details(page_data))
        time.sleep(0.5)

    df = pd.DataFrame(all_data)

    return df


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


def extract_facilities_data(**kwargs):
    api_url = 'http://kopis.or.kr/openApi/restful/prfplc'
    api_key = Variable.get('kopis_api_key_facility')
    api_key_decode = requests.utils.unquote(api_key)
    seoul_area_code = 11
    params = {
        'service': api_key_decode,
        'rows': 1000,
        'signgucode': seoul_area_code,
    }

    fetched_df = fetch_all_facilities(api_url, params)
    print(f'Fetched {len(fetched_df)} items')

    return fetched_df.to_json()


def extract_facility_detail_data(**kwargs):
    api_url = 'http://kopis.or.kr/openApi/restful/prfplc'
    api_key = Variable.get('kopis_api_key_facility')
    api_key_decode = requests.utils.unquote(api_key)
    params = {
        'service': api_key_decode
    }

    df_json = kwargs['ti'].xcom_pull(task_ids='extract_facilities_data')
    df = pd.read_json(df_json)

    fetched_df = fetch_all_facility_details(api_url, df['mt10id'], params)
    print(f'Fetched {len(fetched_df)} items')

    return fetched_df.to_json()


def load_to_s3_raw(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-raw-layer'

    # # Pull merged DataFrame from XCom
    facility_list_json = ti.xcom_pull(task_ids='extract_facilities_data')
    df_facility_list = pd.read_json(facility_list_json)
    facility_detail_json = ti.xcom_pull(task_ids='extract_facility_detail_data')
    df_facility_detail = pd.read_json(facility_detail_json)

    # Convert DataFrame to CSV string
    facility_list_csv_string = convert_to_csv_string(df_facility_list)
    facility_detail_csv_string = convert_to_csv_string(df_facility_detail)

    # Define S3 path
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    facility_list_s3_key = f'source/kopis/facilities/{kst_date.year}/{kst_date.month}/{kst_date.day}/facilities_{kst_date.strftime("%Y%m%d")}.csv'
    facility_detail_s3_key = f'source/kopis/facility_detail/{kst_date.year}/{kst_date.month}/{kst_date.day}/facility_detail{kst_date.strftime("%Y%m%d")}.csv'

    # Upload to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_to_s3(s3, facility_list_s3_key, facility_list_csv_string, bucket)
    upload_to_s3(s3, facility_detail_s3_key, facility_detail_csv_string, bucket)


def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)


def convert_to_csv_string(df):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def upload_to_s3(s3, s3_key,csv_string, bucket):
    s3.load_string(
        string_data=csv_string,
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
    'update_facilities_data',
    default_args=default_args,
    description='A DAG to update facilities data every day and save it to S3',
    schedule_interval='@weekly',
    start_date=days_ago(7),
    catchup=False,
)

extract_facilities_data = PythonOperator(
    task_id='extract_facilities_data',
    python_callable=extract_facilities_data,
    dag=dag,
)

extract_facility_detail_data = PythonOperator(
    task_id='extract_facility_detail_data',
    python_callable=extract_facility_detail_data,
    dag=dag,
)

load_to_s3_raw = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3_raw,
    dag=dag,
)

extract_facilities_data >> extract_facility_detail_data >>load_to_s3_raw