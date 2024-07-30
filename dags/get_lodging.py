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
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import logging
import requests
import json


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
    
    # s3 클라이언트를 생성 -> S3에 저장
    s3 = s3_connection()
    bucket_name = 'hellokorea-raw-layer'
    file_name = 'lodging.json'

    with open(file_name, 'w') as f:
        json.dump(lodgings, f)

    s3.upload_file(file_name, bucket_name, 'source/lodging/lodging.json')
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
        TRDSTATEGBN = lodging["TRDSTATEGBN"]    # 영업상태코드
        if TRDSTATEGBN in ['03']:
            continue
        UPTAENM = lodging["UPTAENM"]  # 업태구분명
        if "호텔" not in UPTAENM:
            continue
        MGTNO = lodging["MGTNO"]      # 관리번호
        RDNWHLADDR = lodging["RDNWHLADDR"]       # 도로명주소
        match = RDNWHLADDR.split()
        SIGUNGUCODE = int(areas_dic.get(match[1]))   # 시군구 코드        
        BPLCNM1 = lodging["BPLCNM"]     # 사업장명
        BPLCNM = BPLCNM1.replace("'", "")
        lo1 = lodging["X"] or '1297744.1643021935'
        lo2 = lo1.replace(" ", "")  # x좌표정보 (경도)
        la1 = lodging["Y"] or '485229.0867525645'
        la2 = la1.replace(" ", "")  # y좌표정보 (위도)
        lo, la = convert_tm_to_wgs84(lo2, la2)  #중부원점TM (EPSG:2097) 좌표를 GRS80 (WGS84) 좌표계로 변환
        records.append([MGTNO, RDNWHLADDR, SIGUNGUCODE, BPLCNM, UPTAENM, lo, la])
    logging.info("parsing done")
    return records


# 3-1. 파싱한 데이터를 RDS에 적재
@task
def parsing_data_to_RDS(records, table):
    connection = get_RDS_connection()
    cursor = connection.cursor()
    
    # 리스트 데이터 삽입 SQL 실행
    try:
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        cursor.execute("BEGIN;")
        cursor.execute(f"""TRUNCATE TABLE {table};""")
        for r in records:
            sql = f"""INSERT INTO {table} ("mgtno" , "rdnwhladdr", "bplcnm", "uptaenm", "addCode", "lo", "la")
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
            cursor.execute(sql, (r[0], r[1], r[3], r[4], r[2], r[5], r[6]))
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
def parsing_data_to_stage(records):
    df = pd.DataFrame(records, columns=['MGTNO', 'RDNWHLADDR', 'SIGUNGUCODE', 'BPLCNM', 'UPTAENM', 'lo', 'la'])
    table = pa.Table.from_pandas(df)    # DataFrame을 Apache Arrow 테이블로 변환
    parquet_file = 'lodging.parquet'
    pq.write_table(table, parquet_file)
    
    s3_client = s3_connection()
    with open(parquet_file, 'rb') as f:
        s3_client.upload_fileobj(f, 'hellokorea-stage-layer', 'source/lodging/lodging.parquet')
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
    schedule = '0 0 * * 1' # 월요일 자정에 실행
) as dag:
    api_task = call_api()
    parsing_task = get_and_parsing()
    stage_task = parsing_data_to_stage(parsing_task)
    RDS_task = parsing_data_to_RDS(parsing_task, "tour_lodging")
    load_task = load_to_redshift("raw_data", "lodging")

api_task >> parsing_task >> RDS_task, stage_task >> load_task
