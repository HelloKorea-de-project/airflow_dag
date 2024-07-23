from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable

import pytz
import io

import requests
import logging
import pandas as pd
import json

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


def extract_tour_attractions_data(**kwargs):
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

    return fetched_df.to_json()



def load_to_s3_raw(**kwargs):
    ti = kwargs['ti']
    bucket = 'hellokorea-raw-layer'

    # # Pull merged DataFrame from XCom
    tour_data_json = ti.xcom_pull(task_ids='extract_tour_attractions_data')
    df_tour = pd.read_json(tour_data_json)

    # Convert DataFrame to CSV string
    csv_string = convert_to_csv_string(df_tour)

    # Define S3 path
    execution_date = kwargs['execution_date']
    kst_date = convert_to_kst(execution_date)
    logging.info(f'excution date: {execution_date}')
    logging.info(f'kst_date: {kst_date}')
    s3_key = f'source/tour/attractions/{kst_date.year}/{kst_date.month}/{kst_date.day}/tour_attractions_{kst_date.strftime("%Y%m%d")}.csv'
    logging.info(f's3_key will be loaded: {s3_key}')

    # Upload to S3
    s3 = S3Hook(aws_conn_id='s3_conn')
    upload_to_s3(s3, s3_key, csv_string, bucket)


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
    'update_to_raw_tour_attractions_data',
    default_args=default_args,
    description='A DAG to update tour attractions data every day and save it to S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

extract_tour_attractions_data = PythonOperator(
    task_id='extract_tour_attractions_data',
    python_callable=extract_tour_attractions_data,
    dag=dag,
)

load_to_s3_raw = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_to_s3_raw,
    dag=dag,
)

trigger_stage_tour_dag = TriggerDagRunOperator(
    task_id='trigger_stage_tour_dag',
    trigger_dag_id='stage_tour_attractions_data',
    execution_date='{{ ds }}',
    reset_dag_run=True,
    dag=dag,
)
extract_tour_attractions_data >> load_to_s3_raw >> trigger_stage_tour_dag