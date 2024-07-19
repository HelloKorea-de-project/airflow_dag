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


def fetch_all_performances(api_url, params=None):
    all_data = []
    page = 1

    while True:
        params['cpage'] = page
        logging.info(f'Fetching page {page}')
        page_data = fetch_page(api_url, params)

        if not page_data or '<db>' not in page_data:
            break

        all_data.extend(parse_xml_performances(page_data))

        page += 1

    df = pd.DataFrame(all_data)

    return df


def fetch_all_performance_details(api_url, perf_ids, params=None):
    all_data = []

    for idx, perf_id in enumerate(perf_ids, start=1):
        detail_api_url = api_url + f'/{perf_id}'
        logging.info(f'Fetching {idx}th item {perf_id}')
        page_data = fetch_page(detail_api_url, params)

        if not page_data or '<db>' not in page_data:
            break

        all_data.extend(parse_xml_performance_details(page_data))
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


def parse_xml_performance_details(xml_data):
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


def extract_performance_data(**kwargs):
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

    fetched_df = fetch_all_performances(api_url, params)
    print(f'Fetched {len(fetched_df)} items')

    return fetched_df.to_json()


def extract_performance_detail_data(**kwargs):
    api_url = 'https://www.kopis.or.kr/openApi/restful/pblprfr'
    performance_api_key = Variable.get('kopis_api_key_performance')
    api_key_decode = requests.utils.unquote(performance_api_key)
    params = {
        'service': api_key_decode
    }

    df_json = kwargs['ti'].xcom_pull(task_ids='extract_performance_data')
    df = pd.read_json(df_json)

    fetched_df = fetch_all_performance_details(api_url, df['mt20id'], params)
    print(f'Fetched {len(fetched_df)} items')

    return fetched_df.to_json()



def load_to_s3_raw(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-raw-layer'

    # # Pull merged DataFrame from XCom
    performance_data_json = ti.xcom_pull(task_ids='extract_performance_data')
    df_perf_list = pd.read_json(performance_data_json)
    performance_detail_data_json = ti.xcom_pull(task_ids='extract_performance_detail_data')
    df_perf_detail = pd.read_json(performance_detail_data_json)

    # Convert DataFrame to CSV string
    perf_list_csv_string = convert_to_csv_string(df_perf_list)
    perf_detail_csv_string = convert_to_csv_string(df_perf_detail)

    # Define S3 path
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    perf_list_s3_key = f'source/kopis/performance/{kst_date.year}/{kst_date.month}/{kst_date.day}/performance_{kst_date.strftime("%Y%m%dT%H%M%S")}.csv'
    perf_detail_s3_key = f'source/kopis/performance_detail/{kst_date.year}/{kst_date.month}/{kst_date.day}/performance_detail_{kst_date.strftime("%Y%m%dT%H%M%S")}.csv'

    # Upload to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_to_s3(s3, perf_list_s3_key, perf_list_csv_string, bucket)
    upload_to_s3(s3, perf_detail_s3_key, perf_detail_csv_string, bucket)


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
    'update_performance_data',
    default_args=default_args,
    description='A DAG to update performance data every day and save it to S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

extract_performance_data = PythonOperator(
    task_id='extract_performance_data',
    python_callable=extract_performance_data,
    dag=dag,
)

extract_performance_detail_data = PythonOperator(
    task_id='extract_performance_detail_data',
    python_callable=extract_performance_detail_data,
    dag=dag,
)

load_to_s3_raw = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3_raw,
    dag=dag,
)

extract_performance_data >> extract_performance_detail_data >> load_to_s3_raw