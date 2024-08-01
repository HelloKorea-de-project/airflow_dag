from airflow.decorators import dag,task
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine
from apify_client import ApifyClient

from datetime import datetime, timedelta

import requests, logging, json, psycopg2, asyncio, time
import pandas as pd
from io import BytesIO, StringIO

from plugins import slack


default_args = {
        'owner':'yjshin',
        'start_date' : datetime(2024,7,29,2,15),
        'retries':1,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack.on_failure_callback
}

@dag(
    dag_id='flight_price_v1',
    schedule_interval = timedelta(days=3),
    max_active_runs = 1,
    default_args=default_args,
    catchup=False,
    tags=['yjshin','flight_price'])


def dag():
    def get_redshift_connection():
        hook = PostgresHook(postgres_conn_id = "redshift_conn")
        return hook.get_conn().cursor()
    
    def get_prod_connection():
        hook = PostgresHook(postgres_conn_id = "postgres_conn")
        return hook.get_conn().cursor()

    @task
    def get_high_frequency_airports():
        """
        Tasks to get the number of flight plans
        by departure airport table (arrcounttoicn)
        and airport information table (serviceairporticn)
        
        input : 
        """

        cur = get_redshift_connection()
        bring_departures_query = f"""
            SELECT a.airportcode, s.countrycode, s.currencycode
            FROM raw_data.arrcounttoicn a
            LEFT JOIN raw_data.serviceairporticn s
                ON a.airportcode = s.airportcode
            WHERE a.isExpired = False 
                AND s.isExpired = False 
                AND s.countrycode IS NOT NULL
                AND s.currencycode IS NOT NULL
            ORDER BY a.count DESC
            LIMIT 30;
        """
        cur.execute(bring_departures_query)
        rows = cur.fetchall()
        
        departures = []
        for row in rows:
            airport = row[0]
            country = row[1]
            currency = row[2]
            departures.append([airport, country, currency])
        
        return departures
    
    def api_actor_run(run_input, flight_price_api):
        try:
            endpoint = f"https://api.apify.com/v2/acts/jupri~skyscanner-flight/runs?token={flight_price_api}"
            response = requests.post(endpoint, json=run_input)
            time.sleep(0.3)
            datasetId = response.json()["data"]["defaultDatasetId"]
            return datasetId
        except Exception as e:
            logging.info("api actor error")
            raise e
        
    @task
    def api_call(departures, search_date):
        logging.info("api call start")
        flight_price_api = Variable.get("flight_price_api")
        
        results = []
        for airport, country, currency in departures:
            depAirportCode = airport
            depCountryCode = country
            currencyCode = currency
            for days in range(1, 31):
                nowSearchDate = search_date[f'date_{days}']
                print(depAirportCode, nowSearchDate)
                run_input = {
                    "alternate_origin": False,
                    "alternate_target": False,
                    "cabin_class": "economy",
                    "dev_dataset_clear": False,
                    "dev_no_strip": False,
                    "dev_proxy_config": {
                    "useApifyProxy": False
                    },
                    "non_stop": True,
                    "market": depCountryCode,
                    "currency": currencyCode,
                    "origin.0": depAirportCode,
                    "target.0": "ICN",
                    "depart.0": nowSearchDate,
                }
                results.append([depAirportCode, depCountryCode, currencyCode, nowSearchDate, api_actor_run(run_input, flight_price_api)])
            time.sleep(150)
        logging.info("api call success")
        return results
        
    @task
    def data_to_raw(dataset_list, current_date):
        """
        load api data to raw layer
        """
        logging.info("start load to raw layer")
        time.sleep(60) # to wait dataset completed
        
        flight_price_api = Variable.get("flight_price_api")
        client = ApifyClient(flight_price_api)
        s3_hook = S3Hook(aws_conn_id="s3_conn")
        for depAirportCode, depCountryCode, currencyCode, nowSerachDate, datasetId in dataset_list:
            data_to_load = [item for item in client.dataset(datasetId).iterate_items()]

            s3_key = f"source/flight/{depAirportCode}_to_ICN_{nowSerachDate}_updated_{current_date}.json"
            s3_bucket = "hellokorea-raw-layer"

            s3_hook.load_string(
                string_data = json.dumps(data_to_load),
                key = s3_key,
                bucket_name = s3_bucket,
                replace = True
            )

        logging.info(f"loaded raw layer")

                
    async def transform(s3_key_raw, depAirportCode, depCountryCode, currencyCode, current_date):
        """
        extract data from raw layer
        to store in the stage layer after preprocessing
        """
        s3_hook = S3Hook(aws_conn_id="s3_conn")
        
        file_content = s3_hook.read_key(key=s3_key_raw, bucket_name='hellokorea-raw-layer')
        df = pd.read_json(StringIO(file_content))

        if not df.empty:
            df = pd.json_normalize(df.to_dict(orient='records'))

            tmp_date = datetime.strptime(current_date, '%Y-%m-%d')
            results = []

            print(len(df["id"]), len(df["pricing_options"]))
            # Preprocess (extract departure airport, arrival airport, airline, departure time, price, URL, update date)
            for idx, (id, price) in enumerate(zip(df["id"], df["pricing_options"])):
                tmp = list(map(str, id.split('-')))
                depTime = datetime.strptime(f"20{tmp[1][:2]}-{tmp[1][2:4]}-{tmp[1][4:6]} {tmp[1][6:8]}:{tmp[1][8:10]}", "%Y-%m-%d %H:%M")
                carrierCode = '-' + tmp[3]
                carrierName = df.loc[idx][f"_carriers.{carrierCode}.name"]
                arrAirportCode, arrTime = 'ICN', datetime.strptime(f"20{tmp[6][:2]}-{tmp[6][2:4]}-{tmp[6][4:6]} {tmp[6][6:8]}:{tmp[6][8:10]}", "%Y-%m-%d %H:%M")
                cheapest_price = int(round(price[0]["items"][0]["price"]["amount"],0))
                url = price[0]["items"][0]["url"]
                extractedDate = tmp_date
                isExpired = False

                results.append([id, depAirportCode, depCountryCode, currencyCode, arrAirportCode, carrierName, depTime, arrTime, cheapest_price, url, extractedDate, isExpired])

            # List to parquet
            col = ['id', 'depAirportCode', 'depCountryCode', 'currencyCode', 'arrAirportCode', 'carrierName', 'depTime', 'arrTime', 'price', 'url', 'extractedDate', 'isExpired']
            df = pd.DataFrame(data=results, columns=col)
                
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow', use_deprecated_int96_timestamps=True)
            parquet_buffer.seek(0)
                
            s3_key_transformed = s3_key_raw[:-4] + 'parquet'
            s3_bucket_transformed = 'hellokorea-stage-layer'
            s3_hook.load_bytes(
                bytes_data = parquet_buffer.getvalue(),
                key = s3_key_transformed,
                bucket_name = s3_bucket_transformed,
                replace=True
            )
        
    async def extract_from_s3(dataset_list, current_date):
        tasks = []
        
        for depAirportCode, depCountryCode, currencyCode, nowSearchDate, datasetId in dataset_list:
            s3_key_raw = f"source/flight/{depAirportCode}_to_ICN_{nowSearchDate}_updated_{current_date}.json"
            task = asyncio.create_task(transform(s3_key_raw, depAirportCode, depCountryCode, currencyCode, current_date))
            tasks.append(task)
                
        await asyncio.gather(*tasks)
        logging.info("transformed done")

    @task
    def raw_to_stage(dataset_list, current_date):
        """
        Get raw data from the raw layer, preprocess it,
        and load it into the stage layer
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(extract_from_s3(dataset_list, current_date))
            
    def update_redshift_query(cur, s3_path):
        """
        parquet to redshift (table: cheapestflight)
        """
        iam_role = "arn:aws:iam::862327261051:role/hellokorea_redshift_s3_access_role"
        
        schema = "raw_data"
        table = "CheapestFlight"
        
        try:
            cur.execute("BEGIN;")
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
        
    @task
    def update_redshift(dataset_list, current_date):
        logging.info("update redshift started")
        cur = get_redshift_connection()
        iam_role = "arn:aws:iam::862327261051:role/hellokorea_redshift_s3_access_role"
        
        schema = "raw_data"
        table = "CheapestFlight"
        try:
            cur.execute("BEGIN;")
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    id VARCHAR(255) primary key NOT NULL,
                    depAirportCode VARCHAR(10),
                    depCountryCode VARCHAR(10),
                    currencyCode VARCHAR(10),
                    arrAirportCode VARCHAR(10),
                    carrierName VARCHAR(50),
                    depTime TIMESTAMP,
                    arrTime TIMESTAMP,
                    price BIGINT,
                    url VARCHAR(800),
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
            
            update_query = f"""
                UPDATE {schema}.{table}
                SET isExpired = True
                WHERE extractedDate < '{current_date}';
            """
            cur.execute(update_query)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise e
            
        for depAirportCode, depCountryCode, currencyCode, nowSearchDate, datasetId in dataset_list:
            s3_key_to_redshift = f"source/flight/{depAirportCode}_to_ICN_{nowSearchDate}_updated_{current_date}.parquet"
            s3_bucket = "hellokorea-stage-layer"
            s3_path = f"s3://{s3_bucket}/{s3_key_to_redshift}"
            
            update_redshift_query(cur, s3_path)
            print(f"{s3_path} updated")
            
        if cur:
            cur.close()
            cur.connection.close()
        logging.info("update redshift ended")
        
        
    @task
    def unload_redshift_to_s3(current_date):
        logging.info("unload redshift to s3")
        cur = get_redshift_connection()
        iam_role = "arn:aws:iam::862327261051:role/hellokorea_redshift_s3_access_role"
        
        schema = "raw_data"
        table = "CheapestFlight"
        
        s3_key_to_unload = f"source/flight/cheapest_flight_to_ICN_updated_{current_date}.csv"
        s3_bucket = "hellokorea-external-zone"
        s3_path = f"s3://{s3_bucket}/{s3_key_to_unload}"
        
        try:
            cur.execute("BEGIN;")
            unload_query = f"""
                UNLOAD
                    ('SELECT id, depAirportCode, depCountryCode, currencyCode, arrAirportCode, carrierName, depTime, arrTime, price, url
                    FROM {schema}.{table}
                    WHERE isExpired = False')
                TO '{s3_path}'
                IAM_ROLE '{iam_role}'
                PARALLEL OFF
                CSV;
            """
            cur.execute(unload_query)
            cur.execute("COMMIT;")
            
            return s3_key_to_unload
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise e
        
        finally:
            if cur:
                cur.close()
                cur.connection.close()
        
    @task
    def update_rds(s3_key_to_prod):
        cur = get_prod_connection()
        
        s3_bucket = "hellokorea-external-zone"
        table = "airline_cheapestflight"
        
        try:
            cur.execute("BEGIN;")
            truncate_query = f"""
                TRUNCATE TABLE {table};
            """
            cur.execute(truncate_query)
            
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")
                
            copy_query = f"""
                SELECT aws_s3.table_import_from_s3(
                    '{table}', 
                    '"id", "depAirportCode", "depCountryCode", "currencyCode", "arrAirportCode", "carrierName", "depTime", "arrTime", "price", "url"',
                    '(format csv)',
                    '{s3_bucket}',
                    '{s3_key_to_prod}000',
                    'ap-northeast-2'
                );
            """
            cur.execute(copy_query)
            cur.execute("COMMIT;")
            
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise e
        
        finally:
            if cur:
                cur.close()
                cur.connection.close()
    
                
    current_date = '{{ ds }}'
    search_date = {f'date_{days}': f'{{{{ macros.ds_add(ds, {days}) }}}}' for days in range(1, 31)} # dictionary
    departures = get_high_frequency_airports()
    dataset_list = api_call(departures, search_date)
    data_to_raw(dataset_list, current_date)
    raw_to_stage(dataset_list, current_date)
    update_redshift(dataset_list, current_date)
    s3_key_to_prod = unload_redshift_to_s3(current_date)
    update_rds(s3_key_to_prod)
    
dag=dag()
