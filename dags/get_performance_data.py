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


def extract_performance_data(**kwargs):
    api_url = 'https://www.kopis.or.kr/openApi/restful/pblprfr'
    performance_api_key = Variable.get('kopis_api_key')
    api_key_decode = requests.utils.unquote(performance_api_key)
    stdate = (kwargs['execution_date'] + timedelta(days=1)).strftime("%Y%m%d")
    eddate = (kwargs['execution_date'] + timedelta(days=30)).strftime("%Y%m%d")
    logging.info(f'{stdate} to {eddate}')
    params = {
        'service': api_key_decode,
        'stdate': stdate,
        'eddate': eddate,
        'rows': 100
    }

    fetched_df = fetch_all_performances(api_url, params)
    print(f'Fetched {len(fetched_df)} items')

    return fetched_df.to_json()



def load_to_s3_raw(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-raw-layer'

    # # Pull merged DataFrame from XCom
    performance_data_json = ti.xcom_pull(task_ids='extract_performance_data')
    df = pd.read_json(performance_data_json)

    # Convert DataFrame to CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()

    # Define S3 path
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    s3_key = f'source/kopis/performance/{kst_date.year}/{kst_date.month}/{kst_date.day}/performance_{kst_date.strftime("%Y%m%dT%H%M%S")}.csv'

    # Upload to S3
    s3 = S3Hook(aws_conn_id='aws_s3')
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

load_to_s3_raw = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3_raw,
    dag=dag,
)

extract_performance_data >> load_to_s3_raw