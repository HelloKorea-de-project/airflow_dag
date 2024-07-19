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


def fetch_all_pages(api_url, params=None):
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


def extract_facilities_data(**kwargs):
    api_url = 'http://kopis.or.kr/openApi/restful/prfplc'
    api_key = Variable.get('kopis_api_key')
    api_key_decode = requests.utils.unquote(api_key)
    seoul_area_code = 11
    params = {
        'service': api_key_decode,
        'rows': 1000,
        'signgucode': seoul_area_code,
    }

    fetched_df = fetch_all_pages(api_url, params)
    print(f'Fetched {len(fetched_df)} items')

    return fetched_df.to_json()



def load_to_s3_raw(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-raw-layer'

    # # Pull merged DataFrame from XCom
    facilities_data_json = ti.xcom_pull(task_ids='extract_facilities_data')
    df = pd.read_json(facilities_data_json)

    # Convert DataFrame to CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()

    # Define S3 path
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    s3_key = f'source/kopis/facilities/{kst_date.year}/{kst_date.month}/{kst_date.day}/facilities_{kst_date.strftime("%Y%m%dT%H%M%S")}.csv'

    # Upload to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    s3.load_string(
        string_data=csv_string,
        key=s3_key,
        bucket_name=bucket
    )


def convert_to_kst(execution_date):
    kst = pytz.timezone('Asia/Seoul')
    return execution_date.astimezone(kst)


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

load_to_s3_raw = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3_raw,
    dag=dag,
)

extract_facilities_data >> load_to_s3_raw