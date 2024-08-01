from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine

from datetime import datetime, timedelta

import requests, logging, json, psycopg2
import pandas as pd
from io import BytesIO

default_args = {
        'owner': 'yjshin',
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
}

@dag(
    dag_id = 'service_airport_ICN_v1',
    start_date = datetime(2024,7,28,15,0),
    schedule = timedelta(days=10),
    max_active_runs = 1,
    catchup = False,
    default_args=default_args,
    tags=['yjshin','service_airport'])

def dag():
    def get_redshift_connection():
        hook = PostgresHook(postgres_conn_id = "redshift_conn")
        return hook.get_conn().cursor()
        
    @task
    def api_to_s3_raw(current_date):
        service_airport_api = Variable.get("service_airport_api")
        
        url = f"http://apis.data.go.kr/B551177/StatusOfSrvDestinations/getServiceDestinationInfo?serviceKey={service_airport_api}&type=json"

        try:
            response = requests.get(url)
            response.raise_for_status()
            
            if response.status_code == 200:
                contents = response.text
                data = json.loads(contents)['response']['body']['items']
                
        except Exception as e:
            logging.info(f"API call error: {e}")
            raise e

        s3_hook = S3Hook(aws_conn_id="s3_conn")
        s3_key = f"source/flight/service_airport_to_ICN_updated_{current_date}.json"
        s3_bucket = "hellokorea-raw-layer"

        s3_hook.load_string(
            string_data = json.dumps(data, ensure_ascii=False),
            key = s3_key,
            bucket_name = s3_bucket,
            replace = True
        )
        logging.info("api call success")
        return s3_key

    @task
    def transform(current_date, s3_key_raw):
        s3_hook = S3Hook(aws_conn_id="s3_conn")
        s3_key = s3_key_raw
        s3_bucket = "hellokorea-raw-layer"
        service_airport_obj = s3_hook.get_key(
            key=s3_key, 
            bucket_name= s3_bucket
        )
        service_airport = service_airport_obj.get()["Body"].read()
        airport_information_obj = s3_hook.get_key(
            key = "source/flight/airport_information.parquet",
            bucket_name = "hellokorea-stage-layer"
        )
        airport_information = airport_information_obj.get()["Body"].read()
        
        df1 = pd.read_json(BytesIO(service_airport))
        df1 = df1["airportCode"]
        
        df2 = pd.read_parquet(BytesIO(airport_information))
        try:
            df3 = pd.merge(df1, df2, on='airportCode', how='left')
            df3["extractedDate"] = datetime(int(current_date[:4]),int(current_date[5:7]),int(current_date[8:10]))
            df3["isExpired"] = False

            re_col = ["airportCode","countryCode","currencyCode","extractedDate","isExpired"]
            df3 = df3[re_col]
            
            parquet_buffer = BytesIO()
            df3.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            s3_key = s3_key[:-4]+"parquet"
            s3_hook.load_bytes(
                bytes_data = parquet_buffer.getvalue(),
                key = s3_key,
                bucket_name="hellokorea-stage-layer",
                replace = True
            )
            logging.info("loaded to stage layer")
        except Exception as e:
            logging.info("merge failed")
            raise e
        
        return s3_key

    @task
    def update_redshift(current_date, s3_key_transformed):
        s3_bucket = "hellokorea-stage-layer"
        s3_path = f"s3://{s3_bucket}/{s3_key_transformed}"
        
        cur = get_redshift_connection()
        schema = 'raw_data'
        table = 'ServiceAirportICN'
        
        try:
            cur.execute("BEGIN;")
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    airportCode VARCHAR(10) NOT NULL,
                    countryCode VARCHAR(10),
                    currencyCode VARCHAR(10),
                    extractedDate TIMESTAMP,
                    isExpired BOOLEAN
                );
            """
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
            cur.execute("ROLLBACK;")
            raise e
        
        logging.info("redshift updated")

    current_date = '{{ macros.ds_add(ds, 10) }}'
    s3_key_raw = api_to_s3_raw(current_date)
    s3_key_transformed = transform(current_date, s3_key_raw)
    update_redshift(current_date, s3_key_transformed)
    

dag = dag()
