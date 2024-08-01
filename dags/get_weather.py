'''
* 기상청 API 사용 DAG 작성*
'''


from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
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


# 1. API 호출 및 S3에 저장
@task
def call_api():

    # API 호출
    params = {'numOfRows': '380', 'dataType': 'JSON', 'dataCd': 'ASOS', 'dateCd': 'DAY', 'startDt': 20230731, 'endDt': 20240730, 'stnIds': '108'}
    response = requests.get(Variable.get("weather_api_key"), params=params)
    data = response.json()

    # S3에 저장
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    bucket_name = 'hellokorea-raw-layer'
    s3_hook.load_string(
                string_data = json.dumps(data),
                key = 'source/weather/2023/7/weather.json',
                bucket_name = bucket_name,
                replace = True
            )
    logging.info("s3 upload done")


# 2. S3에서 JSON 파일을 가져오고 파싱
@task
def get_and_parsing():

    s3_client = s3_connection()
    bucket_name = 'hellokorea-raw-layer'
    file_name = 'source/weather/2023/7/weather.json'

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
        cursor.execute(f"""TRUNCATE TABLE {table};""")
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
    s3_client.upload_fileobj(buffer, 'hellokorea-stage-layer', 'source/weather/2023/7/weather.parquet') # BytesIO 버퍼의 내용을 S3에 업로드

    logging.info("weather.parque to S3 stage layer")


# 4. 테이블 형식으로 Redshift 적재
@task
def load_to_redshift(schema, table):

    connection = get_Redshift_connection()
    cursor = connection.cursor()
    iam_role = Variable.get("hellokorea_redshift_s3_access_role")

    try:
        # FULL REFRESH
        cursor.execute("BEGIN;")
        cursor.execute(f"""TRUNCATE TABLE {schema}.{table};""")
        cursor.execute(f"""
            COPY {schema}.{table}
            FROM 's3://hellokorea-stage-layer/source/weather/2023/7/weather.parquet'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
            """)
        cursor.execute("COMMIT;")
        logging.info(f'Successfully loaded data from S3 to Redshift table {schema}.{table}')
    except Exception as e:
        print(f'Error: {e}')
        cursor.execute("ROLLBACK;")
    finally:
        cursor.close()
        connection.close()
    logging.info("load done")


with DAG(
    dag_id = 'weather',
    start_date = datetime(2024,7,31),
    catchup=False,
    tags=['API'],
    schedule = '@once', #처음 한번만 실행
    on_failure_callback=slack.on_failure_callback
) as dag:
    api_task = call_api()
    parsing_task = get_and_parsing()
    RDS_task = parsing_data_to_RDS(parsing_task, "airline_weather")
    stage_task = parsing_data_to_stage(parsing_task)
    load_task = load_to_redshift("raw_data", "weather")

api_task >> parsing_task >> RDS_task, stage_task >> load_task