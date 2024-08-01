from airflow.decorators import dag,task
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine

from datetime import datetime

import requests, logging, json, psycopg2

default_args = {
    'owner' : "yjshin",
    'retries' : 1,
}

@dag(
    dag_id = "airport_information_v1",
    start_date = datetime(2024,7,28,15,0),
    schedule = "@once",
    max_active_runs = 1,
    default_args=default_args,
    tags=['yjshin','airport_information']
)

def dag():
    def get_redshift_connection():
        hook = PostgresHook(postgres_conn_id = "redshift_conn")
        return hook.get_conn().cursor()
            
    @task
    def update_redshift(current_date):
        cur = get_redshift_connection()
        schema = 'dimension_data'
        table = 'AirportInformation'
        
        try:
            cur.execute("BEGIN;")
            drop_table_query = f"""
                DROP TABLE IF EXISTS {schema}.{table};
            """
            cur.execute(drop_table_query)
            
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
            
            s3_key = "source/flight/airport_information.parquet"
            s3_bucket = "hellokorea-stage-layer"
            
            s3_path=f"s3://{s3_bucket}/{s3_key}"
            
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
        finally:
            cur.close()
            cur.connection.close()
        logging.info("redshift updated")

    current_date = '{{ ds }}'
    update_redshift(current_date)
    

dag = dag()


