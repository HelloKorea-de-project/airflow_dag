from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from plugins import slack
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import StringIO, BytesIO
import logging

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig, DbtTaskGroup
from cosmos.constants import LoadMode
from pathlib import Path
import os

from plugins.dbt_utils import create_dbt_task_group


# Configuration
TABLE_NAME = 'ExchangeRate'
RAW_LAYER_PATH_TEMPLATE = 'source/koreaexim/exchange_rate/{year}/{month}/{day}/{table_name}_{timestamp}.csv'
STAGE_LAYER_PROD_PATH_TEMPLATE = 'source/koreaexim/exchange_rate/prod_db/{year}-{month}-{day}/{table_name}_{timestamp}.parquet'
STAGE_LAYER_DW_PATH_TEMPLATE = 'source/koreaexim/exchange_rate/dw_db/{year}-{month}-{day}/{table_name}_{timestamp}.parquet'

S3_CONN_ID = 's3_conn'
POSTGRES_CONN_ID = 'postgres_conn'
REDSHIFT_CONN_ID = 'redshift_conn'

prod_columns = {'cur_unit': 'currencyCode', 'deal_bas_r': 'standardRate', 'ttb': 'ttb', 'tts': 'tts'}


def fetch_and_save_csv(logical_date, **kwargs):
    """
    Description:
    Fetches exchange rate data from the Korea Exim API for a given date and saves the data as a CSV file in an S3 bucket.

    Input:
    - logical_date: The date for which to fetch the exchange rate data.

    Output:
    - Saves the fetched data as a CSV file in the specified S3 bucket.
    """
    date_str = logical_date.strftime('%Y%m%d')
    API_URL_TEMPLATE = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&searchdate={date}&data=AP01"
    api_url = API_URL_TEMPLATE.format(api_key=Variable.get(f"koreaexim_{TABLE_NAME.lower()}_secret_key"), date=date_str)

    response = requests.get(api_url, verify=False)
    data = response.json()

    if not data:  # Check if the data is empty
        raise ValueError(f"No data received from API for date: {date_str}")
    else:
        logging.info(f"Data received from API for date: {date_str}, {data}")

    csv_path = generate_path(RAW_LAYER_PATH_TEMPLATE, logical_date)

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    csv_buffer = StringIO()
    df = pd.DataFrame(data)
    df.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), csv_path, bucket_name=Variable.get("S3_RAW_BUCKET"), replace=True)


def convert_csv_to_parquet(logical_date, **kwargs):
    """
    Description:
    Converts the fetched CSV exchange rate data to Parquet format and saves it in an S3 bucket.

    Input:
    - logical_date: The date for which the data was fetched.

    Output:
    - Saves the converted data as a Parquet file in the specified S3 bucket.
    """
    csv_path = generate_path(RAW_LAYER_PATH_TEMPLATE, logical_date)
    stage_path = generate_path(STAGE_LAYER_DW_PATH_TEMPLATE, logical_date)

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    obj = s3_hook.get_key(key=csv_path, bucket_name=Variable.get("S3_RAW_BUCKET"))
    df = pd.read_csv(obj.get()['Body'])

    # Data preprocessing
    df['ttb'] = df['ttb'].replace(',', '', regex=True).astype(float)
    df['tts'] = df['tts'].replace(',', '', regex=True).astype(float)
    df['deal_bas_r'] = df['deal_bas_r'].replace(',', '', regex=True).astype(float)
    df['bkpr'] = df['bkpr'].replace(',', '', regex=True).astype(int)
    df['yy_efee_r'] = df['yy_efee_r'].replace(',', '', regex=True).astype(int)
    df['ten_dd_efee_r'] = df['ten_dd_efee_r'].replace(',', '', regex=True).astype(int)
    df['kftc_bkpr'] = df['kftc_bkpr'].replace(',', '', regex=True).astype(int)
    df['kftc_deal_bas_r'] = df['kftc_deal_bas_r'].replace(',', '', regex=True).astype(float)
    df['created_at'] = pd.to_datetime(datetime.now())

    df['conversion_unit'] = df['cur_unit'].str.extract(r'\((\d+)\)').fillna(1).astype(int)
    df['cur_unit'] = df['cur_unit'].str[:3]

    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer, use_deprecated_int96_timestamps=True)
    buffer.seek(0)
    s3_hook.load_bytes(buffer.getvalue(), stage_path, bucket_name=Variable.get("S3_STAGE_BUCKET"), replace=True)


def clean_raw_to_prod(logical_date, **kwargs):
    """
    Description:
    Cleans the raw exchange rate data by selecting specific columns, renaming them, and converting data types. Saves the cleaned data to S3 in Parquet format.

    Input:
    - logical_date: The date for which the data was fetched.

    Output:
    - Saves the cleaned data as a Parquet file in the specified S3 bucket.
    """
    csv_path = generate_path(RAW_LAYER_PATH_TEMPLATE, logical_date)
    stage_path = generate_path(STAGE_LAYER_PROD_PATH_TEMPLATE, logical_date)

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    obj = s3_hook.get_key(key=csv_path, bucket_name=Variable.get("S3_RAW_BUCKET"))
    df = pd.read_csv(obj.get()['Body'])

    df = df[prod_columns.keys()]

    df['ttb'] = df['ttb'].replace(',', '', regex=True).astype(float)
    df['tts'] = df['tts'].replace(',', '', regex=True).astype(float)
    df['deal_bas_r'] = df['deal_bas_r'].replace(',', '', regex=True).astype(float)

    df = df.rename(columns=prod_columns)
    df_cleaned = df.dropna()

    table = pa.Table.from_pandas(df_cleaned)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    s3_hook.load_bytes(buffer.getvalue(), stage_path, bucket_name=Variable.get("S3_STAGE_BUCKET"), replace=True)


def update_rds(logical_date, **kwargs):
    """
    Description:
    Updates the RDS database with the cleaned exchange rate data by truncating the existing table and inserting new data.

    Input:
    - logical_date: The date for which the data was cleaned.

    Output:
    - Updates the RDS table with the new exchange rate data.
    """
    stage_path = generate_path(STAGE_LAYER_PROD_PATH_TEMPLATE, logical_date)

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    obj = s3_hook.get_key(key=stage_path, bucket_name=Variable.get("S3_STAGE_BUCKET"))
    table = pq.read_table(BytesIO(obj.get()['Body'].read()))
    df = table.to_pandas()

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Clear the table
    cursor.execute(f"TRUNCATE TABLE airline_{TABLE_NAME.lower()};")

    # Insert new data
    columns = ', '.join([f'"{col}"' for col in prod_columns.values()])
    placeholders = ', '.join(['%s'] * len(prod_columns))
    for index, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO airline_{TABLE_NAME.lower()} ({columns})
            VALUES ({placeholders});
        """, [row[col_name] for col_name in prod_columns.values()])

    conn.commit()
    cursor.close()
    conn.close()


def update_redshift(logical_date, **kwargs):
    """
    Description:
    Updates the Redshift database with the cleaned exchange rate data by creating the table if it does not exist and copying the data from S3.

    Input:
    - logical_date: The date for which the data was cleaned.

    Output:
    - Updates the Redshift table with the new exchange rate data.
    """
    stage_path = generate_path(STAGE_LAYER_DW_PATH_TEMPLATE, logical_date)

    redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS raw_data.{TABLE_NAME} (
        result BIGINT,
        cur_unit VARCHAR(10),
        ttb DOUBLE PRECISION,
        tts DOUBLE PRECISION,
        deal_bas_r DOUBLE PRECISION,
        bkpr BIGINT,
        yy_efee_r BIGINT,
        ten_dd_efee_r BIGINT,
        kftc_bkpr BIGINT,
        kftc_deal_bas_r DOUBLE PRECISION,
        cur_nm VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)

    delete_query = f"""
        DELETE FROM raw_data.{TABLE_NAME}
        WHERE created_at = '{logical_date}';
        """
    cursor.execute(delete_query)

    s3_path = f's3://{Variable.get("S3_STAGE_BUCKET")}/{stage_path}'
    iam_role = Variable.get("hellokorea_redshift_s3_access_role")
    query = f"""
        COPY raw_data.{TABLE_NAME}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """
    cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()


def generate_path(template, logical_date):
    """
    Description:
    [Non Task]
    Generates a file path based on a template and a given logical date.

    Input:
    - template: The template string for the path.
    - logical_date: The date to be used in the path.

    Output:
    - Returns the generated file path.
    """
    return template.format(
        year=logical_date.year,
        month=str(logical_date.month).zfill(2),
        day=str(logical_date.day).zfill(2),
        timestamp=logical_date.strftime('%Y%m%dT%H%M%S'),
        table_name=TABLE_NAME
    )


default_args = {
    'owner': 'cshiz',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack.on_failure_callback
}

dag = DAG(
    'exchange_rate_etl',
    default_args=default_args,
    description='ETL process for Korea Exim exchange rate',
    schedule_interval='0 4 * * 1-5',
    catchup=False,
    tags=['prod', 'load-redshift', 'table:exchangerate']
)

fetch_task = PythonOperator(
    task_id='fetch_and_save_csv',
    python_callable=fetch_and_save_csv,
    provide_context=True,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_raw_to_prod',
    python_callable=clean_raw_to_prod,
    provide_context=True,
    dag=dag,
)

convert_task = PythonOperator(
    task_id='convert_csv_to_parquet',
    python_callable=convert_csv_to_parquet,
    provide_context=True,
    dag=dag,
)

update_rds_task = PythonOperator(
    task_id='update_rds',
    python_callable=update_rds,
    provide_context=True,
    dag=dag,
)

update_redshift_task = PythonOperator(
    task_id='update_redshift',
    python_callable=update_redshift,
    provide_context=True,
    dag=dag,
)

dbt_source_test_task_group = create_dbt_task_group(
    group_id="dbt_source_test_task_group",
    select_models=['fresh_ex_rate'],
    dag=dag
)



fetch_task >> [clean_task, convert_task]
clean_task >> update_rds_task
convert_task >> update_redshift_task >> dbt_source_test_task_group

