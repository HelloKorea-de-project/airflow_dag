from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine

from datetime import datetime, timedelta

import requests, logging, json, psycopg2
import pandas as pd
from io import BytesIO
import pyarrow.parquet as pq

default_args = {
        'owner' : "yjshin",
        'start_date' : datetime(2024,7,28,5,0),
        'retries':1,
        'retry_delay': timedelta(minutes=3)
}

@dag(
    dag_id = "arr_count_to_ICN_v1",
    schedule = timedelta(days=10),
    max_active_runs = 1,
    catchup = False,
    default_args=default_args,
    tags=['yjshin','arr_count_to_ICN']
)

def dag():
    def get_redshift_connection():
        hook = PostgresHook(postgres_conn_id="redshift_conn")
        return hook.get_conn().cursor()
    
    @task
    def api_to_stage(current_date):
        arr_count_api = Variable.get("arr_count_api")
        
        cur = get_redshift_connection()
        schema = 'raw_data'
        table = 'ServiceAirportICN'
        
        bring_service_airport_query = f"""
            SELECT airportcode
            FROM {schema}.{table};
        """
        cur.execute(bring_service_airport_query)
        service_airport = cur.fetchall()
        
        data = []
        for airport in service_airport:
            airport = airport[0]
                   
            url = f"http://apis.data.go.kr/B551177/PaxFltSched/getPaxFltSchedArrivals?serviceKey={arr_count_api}&lang=K&airport={airport}&numOfRows=10000&pageNo=1&type=json"
            response = requests.get(url)
            contents = response.json()['response']['body']['items']
            try:
                if response.status_code == 200:
                    data.append([airport, len(contents)])
                    print([airport, len(contents)])
            except Exception as e:
                raise e
        logging.info("api call success")
        
        df2 = pd.DataFrame(data,columns=["airportCode","count"])
        df2["extractedDate"] = datetime.strptime(current_date, '%Y-%m-%d')
        df2["isExpired"] = False
        
        re_col = ["airportCode","count","extractedDate","isExpired"]
        df3 = df2[re_col]
        
        parquet_buffer = BytesIO()
        df3.to_parquet(parquet_buffer, index=False, engine='pyarrow', use_deprecated_int96_timestamps=True)
        parquet_buffer.seek(0)
        
        s3_hook = S3Hook(aws_conn_id="s3_conn")
        s3_key = f"source/flight/arr_count_to_ICN_updated_{current_date}.parquet"
        s3_bucket = "hellokorea-stage-layer"
        
        s3_hook.load_bytes(
            bytes_data = parquet_buffer.getvalue(),
            key = s3_key,
            bucket_name = s3_bucket,
            replace = True
        )
        logging.info("loaded to stage layer")
        
        return s3_key
    
    @task
    def update_redshift(current_date, s3_key_stage):
        s3_bucket = "hellokorea-stage-layer"
        s3_path = f"s3://{s3_bucket}/{s3_key_stage}"
        
        cur = get_redshift_connection()
        schema = 'raw_data'
        table = 'ArrCountToICN'
        
        try:
            cur.execute("BEGIN;")
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    airportCode VARCHAR(10) NOT NULL,
                    count BIGINT,
                    extractedDate TIMESTAMP,
                    isExpired BOOLEAN
            );"""
            cur.execute(create_table_query)
            
            chk_duplicate_extractedDate_query = f"""
                DELETE FROM {schema}.{table}
                WHERE extractedDate = '{current_date}';
            """
            cur.execute(chk_duplicate_extractedDate_query)
            
            expire_query = f"""
                UPDATE {schema}.{table}
                SET isExpired = True
                WHERE extractedDate < '{current_date}';
            """
            cur.execute(expire_query)
            
            iam_role = "arn:aws:iam::862327261051:role/hellokorea_redshift_s3_access_role"
            copy_query = f"""
                COPY {schema}.{table}
                FROM '{s3_path}'
                IAM_ROLE '{iam_role}'
                FORMAT AS PARQUET;
            """
            cur.execute(copy_query)
            cur.execute("COMMIT;")
            
        except Exception as e:
            cur.execute('ROLLBACK;')
            raise e
        
        logging.info("redshift updated")

    current_date = '{{ macros.ds_add(ds, 10) }}'
    s3_key_stage = api_to_stage(current_date)
    update_redshift(current_date, s3_key_stage)

dag=dag()