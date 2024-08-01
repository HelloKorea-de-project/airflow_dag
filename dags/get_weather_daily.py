'''
* 기상청 API 사용 DAG 작성_매일시행*
'''


from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.models import Variable
from plugins import slack
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import logging
import requests
import json
import io


# Airflow Connection에 등록된 Redshift 연결 정보를 가져옴
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    connection = hook.get_conn()
    connection.autocommit = autocommit
    return connection


# Airflow Connection에 등록된 S3 연결 정보를 가져옴
def s3_connection():
    aws_conn_id = 's3_conn'
    s3_hook = S3Hook(aws_conn_id)
    s3_client = s3_hook.get_conn()
    return s3_client


# Airflow Connection에 등록된 RDS 연결 정보를 가져옴
def get_RDS_connection():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    connection = pg_hook.get_conn()
    return connection


# 어제
def get_yesterday():
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%Y%m%d')
    return formatted_yesterday  #20240701


# S3 'hellokorea-raw-layer' path
def s3_rawlayer_path():
    yesterday = datetime.now() - timedelta(days=1)
    return 'source/weather/' + str(yesterday.year) + '/' + str(yesterday.month) + '/' + str(yesterday.day) + '/weather.json'


# S3 'hellokorea-stage-layer' path
def s3_stagelayer_path():
    yesterday = datetime.now() - timedelta(days=1)
    return 'source/weather/' + str(yesterday.year) + '/' + str(yesterday.month) + '/' + str(yesterday.day) + '/weather.parquet'


# 1. API 호출 및 S3에 저장
@task
def call_api():

    # API 호출
    params = {'dataType': 'JSON', 'dataCd': 'ASOS', 'dateCd': 'DAY', 'startDt': get_yesterday(), 'endDt': get_yesterday(), 'stnIds': '108'}
    response = requests.get(Variable.get("weather_api_key"), params=params)
    data = response.json()

    # Boto3 클라이언트를 생성 -> S3에 저장
    s3_client = s3_connection()
    bucket_name = 'hellokorea-raw-layer'
    file_name = 'weather1.json'

    with open(file_name, 'w') as f:
        json.dump(data, f)

    s3_client.upload_file(file_name, bucket_name, s3_rawlayer_path())
    logging.info("s3 upload done")


# 2. S3에서 JSON 파일을 가져오고 파싱
@task
def get_and_parsing():
    s3_client = s3_connection()
    bucket_name = 'hellokorea-raw-layer'
    file_name = s3_rawlayer_path()

    obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    weathers2 = obj['Body'].read().decode('utf-8')
    weathers1 = json.loads(weathers2)
    weathers = weathers1['response']['body']['items']['item']

    records = []

    for weather in weathers:
        tm = weather["tm"]      # 일시
        avgTa = weather["avgTa"]      # 평균기온
        minTa = weather["minTa"]      # 최저기온
        maxTa = weather["maxTa"]      # 최고기온
        sumRn = weather["sumRn"] or '0'    # 일강수량
        records.append([tm, avgTa, minTa, maxTa, sumRn])
    logging.info("parsing done")
    return records


# 3-1. 파싱한 데이터를 RDS에 적재
@task
def parsing_data_to_RDS(records, table):
    connection = get_RDS_connection()
    cursor = connection.cursor()
    
    # 리스트 데이터 삽입 SQL 실행
    try:
        # FULL REFRESH
        cursor.execute("BEGIN;")
        for r in records:
            sql = f"""INSERT INTO {table} ("tm", "avgTa", "minTa", "maxTa", "sumRn")
                VALUES (%s, %s, %s, %s, %s)
                """
            cursor.execute(sql, (r[0], r[1], r[2], r[3], r[4]))
            logging.info(sql, (r[0], r[1], r[2], r[3], r[4]))
        cursor.execute("COMMIT;")
        logging.info(f'Successfully loaded data from S3 to Redshift table {table}')
    except Exception as e:
        print(f'Error: {e}')
        cursor.execute("ROLLBACK;")
    finally:
        cursor.close()
        connection.close()
    logging.info("load done")


# 3-2.파싱한 데이터를 S3-stage layer 적재
@task
def parsing_data_to_stage(records):
    df = pd.DataFrame(records, columns=["tm", "avgTa", "minTa", "maxTa", "sumRn"])  # 리스트를 Pandas DataFrame으로 변환 (열이름 설정)
    df['tm'] = pd.to_datetime(df['tm'], format='%Y-%m-%d').dt.date
    df = df.astype({'avgTa':'float', 'minTa':'float', 'maxTa':'float', 'sumRn':'float'})
    table = pa.Table.from_pandas(df)    # DataFrame을 Apache Arrow 테이블로 변환 

    buffer = io.BytesIO()   # 메모리에서 Parquet 파일을 생성하기 위해 BytesIO 객체 사용
    pq.write_table(table, buffer)   # Arrow 테이블을 BytesIO 객체에 Parquet 파일로 작성 
    buffer.seek(0)  # BytesIO 버퍼의 포인터를 처음으로 되돌리기
    
    s3_client  = s3_connection()
    s3_client.upload_fileobj(buffer, 'hellokorea-stage-layer', s3_stagelayer_path()) # BytesIO 버퍼의 내용을 S3에 업로드

    logging.info("weather.parquet to S3 stage layer")


# 4. 테이블 형식으로 Redshift 적재
@task
def load_to_redshift(schema, table):

    connection = get_Redshift_connection()
    cursor = connection.cursor()
    iam_role = Variable.get("hellokorea_redshift_s3_access_role")

    try:
        # FULL REFRESH
        cursor.execute("BEGIN;")
        cursor.execute(f"""
            COPY {schema}.{table}
            FROM 's3://hellokorea-stage-layer/source/weather/2023/7/weather.parquet'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
            """)
        cursor.execute("COMMIT;")   # cur.execute("END;")
        logging.info(f'Successfully loaded data from S3 to Redshift table {schema}.{table}')
    except Exception as e:
        print(f'Error: {e}')
        cursor.execute("ROLLBACK;")
    finally:
        cursor.close()
        connection.close()
    logging.info("load done")


with DAG(
    dag_id = 'Weather_daily',
    start_date = datetime(2024,7,31),
    catchup=False,
    tags=['API'],
    schedule = '0 3 * * *', #매일 UTC 3시 , KST 12시 시행 (전날 데이터 11시에 업데이트 됨)
    on_failure_callback=slack.on_failure_callback
) as dag:
    api_task = call_api()
    parsing_task = get_and_parsing()
    RDS_task = parsing_data_to_RDS(parsing_task, "airline_weather")
    stage_task = parsing_data_to_stage(parsing_task)
    load_task = load_to_redshift("raw_data", "weather")

api_task >> parsing_task >> stage_task >> load_task