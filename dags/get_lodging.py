'''
* 서울시 숙박업소 API 사용 DAG 작성*
'''


from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from airflow.models import Variable
from pyproj import CRS, Transformer
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


# 중부원점TM 좌표계를 WGS84 좌표계로 변환
def convert_tm_to_wgs84(tm_x, tm_y):
    crs_tm = CRS("EPSG:2097")
    crs_wgs84 = CRS("EPSG:4326")
    transformer = Transformer.from_crs(crs_tm, crs_wgs84, always_xy=True)
    longitude, latitude = transformer.transform(tm_x, tm_y)
    return latitude, longitude


#서울 지역코드
areas_dic = {
    "강남구": "1",
    "강동구": "2",
    "강북구": "3",
    "강서구": "4",
    "관악구": "5",
    "광진구": "6",
    "구로구": "7",
    "금천구": "8",
    "노원구": "9",
    "도봉구": "10",
    "동대문구": "11",
    "동작구": "12",
    "마포구": "13",
    "서대문구": "14",
    "서초구": "15",
    "성동구": "16",
    "성북구": "17",
    "송파구": "18",
    "양천구": "19",
    "영등포구": "20",
    "용산구": "21",
    "은평구": "22",
    "종로구": "23",
    "중구": "24",
    "중랑구": "25",
}


# 1. API 호출 및 S3에 저장
@task
def call_api():

    # API 호출
    lodgings = []
    for i in range(7):
        url = Variable.get("lodging_api_key") + str(i*1000+1) + '/' + str((i+1)*1000) + '/'
        response = requests.get(url)
        lodgings1 = response.json()
        lodgings.extend(lodgings1['LOCALDATA_031103']['row'])
    
    # S3에 저장
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    bucket_name = 'hellokorea-raw-layer'
    s3_hook.load_string(
                string_data = json.dumps(lodgings),
                key = 'source/lodging/lodging.json',
                bucket_name = bucket_name,
                replace = True
            )
    logging.info("s3 upload done")


# 2. S3에서 JSON 파일을 가져오고 파싱
@task
def get_and_parsing():
    s3 = s3_connection()
    bucket_name = 'hellokorea-raw-layer'
    file_name = 'source/lodging/lodging.json'

    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    lodgings1 = obj['Body'].read().decode('utf-8')
    lodgings = json.loads(lodgings1)

    records = []
    for lodging in lodgings:
        trdstategbn = lodging["TRDSTATEGBN"]    # 영업상태코드
        if trdstategbn in ['03']:
            continue
        uptaenm = lodging["UPTAENM"]  # 업태구분명
        if "호텔" not in uptaenm:
            continue
        mgtno = lodging["MGTNO"]      # 관리번호
        rdnwhladdr = lodging["RDNWHLADDR"]       # 도로명주소
        match = rdnwhladdr.split()
        addCode = int(areas_dic.get(match[1]))   # 시군구 코드        
        bplcnm1 = lodging["BPLCNM"]     # 사업장명
        bplcnm = bplcnm1.replace("'", "")
        lo1 = lodging["X"] or '1297744.1643021935'
        lo2 = lo1.replace(" ", "")  # x좌표정보 (경도)
        la1 = lodging["Y"] or '485229.0867525645'
        la2 = la1.replace(" ", "")  # y좌표정보 (위도)
        lo, la = convert_tm_to_wgs84(lo2, la2)  #중부원점TM (EPSG:2097) 좌표를 GRS80 (WGS84) 좌표계로 변환
        records.append([mgtno, rdnwhladdr, addCode, bplcnm, uptaenm, lo, la])
    logging.info("parsing done")
    json_records = json.dumps(records, ensure_ascii=False)
    return json_records


# 3-1. 파싱한 데이터를 RDS에 적재
@task
def parsing_data_to_RDS(json_records, table):
    records = json.loads(json_records)
    connection = get_RDS_connection()
    cursor = connection.cursor()
    
    # 리스트 데이터 삽입 SQL 실행
    try:
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        cursor.execute("BEGIN;")
        cursor.execute(f"""TRUNCATE TABLE {table};""")
        for r in records:
            sql = f"""INSERT INTO {table} ("mgtno" , "rdnwhladdr", "addCode", "bplcnm", "uptaenm", "lo", "la")
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
            cursor.execute(sql, (r[0], r[1], r[2], r[3], r[4], r[5], r[6]))
        cursor.execute("COMMIT;")
        print(f'Successfully loaded data from S3 to Redshift table {table}')
    except Exception as e:
        print(f'Error: {e}')
        cursor.execute("ROLLBACK;")
    finally:
        cursor.close()
        connection.close()
    logging.info("load done")


# 3-2.파싱한 데이터를 S3-stage layer 적재
@task
def parsing_data_to_stage(json_records):
    records = json.loads(json_records)
    df = pd.DataFrame(records, columns=['mgtno', 'rdnwhladdr', 'addCode', 'bplcnm', 'uptaenm', 'lo', 'la'])
    table = pa.Table.from_pandas(df)    # DataFrame을 Apache Arrow 테이블로 변환

    buffer = io.BytesIO()   # 메모리에서 Parquet 파일을 생성하기 위해 BytesIO 객체 사용
    pq.write_table(table, buffer)   # Arrow 테이블을 BytesIO 객체에 Parquet 파일로 작성 
    buffer.seek(0)  # BytesIO 버퍼의 포인터를 처음으로 되돌리기
    
    s3_client  = s3_connection()
    s3_client.upload_fileobj(buffer, 'hellokorea-stage-layer', 'source/lodging/lodging.parquet') # BytesIO 버퍼의 내용을 S3에 업로드
    logging.info("lodging.parquet to S3 stage layer")


# 4. 테이블 형식으로 Redshift 적재
@task
def load_to_redshift(schema, table):

    connection = get_Redshift_connection()
    cursor = connection.cursor()
    iam_role = Variable.get("hellokorea_redshift_s3_access_role")

    try:
        # FULL REFRESH
        cursor.execute("BEGIN;")
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
            mgtno VARCHAR(30),
            rdnwhladdr  VARCHAR(150),
            addCode BIGINT,
            bplcnm  VARCHAR(100),
            uptaenm VARCHAR(30),
            lo FLOAT,
            la FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""")
        cursor.execute(f"""TRUNCATE TABLE {schema}.{table};""")
        cursor.execute(f"""
            COPY {schema}.{table}
            FROM 's3://hellokorea-stage-layer/source/lodging/lodging.parquet'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
            """)
        cursor.execute("COMMIT;")
        print(f'Successfully loaded data from S3 to Redshift table {schema}.{table}')
    except Exception as e:
        print(f'Error: {e}')
        cursor.execute("ROLLBACK;")
    finally:
        cursor.close()
        connection.close()
    logging.info("load done")


with DAG(
    dag_id = 'lodging',
    start_date = datetime(2024,7,20),
    catchup=False,
    tags=['API'],
    schedule = '0 0 * * 1', # 월요일 자정에 실행
    on_failure_callback = slack.on_failure_callback
) as dag:
    api_task = call_api()
    parsing_task = get_and_parsing()
    stage_task = parsing_data_to_stage(parsing_task)
    RDS_task = parsing_data_to_RDS(parsing_task, "tour_lodging")
    load_task = load_to_redshift("raw_data", "lodging")

api_task >> parsing_task >> RDS_task, stage_task >> load_task
