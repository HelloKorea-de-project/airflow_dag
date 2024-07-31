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

default_args = {
        'owner':'yjshin',
        'start_date' : datetime(2024,7,28,18,0)
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
        except:
            return False
        
    @task
    def api_call(departures, search_date):
        logging.info("api call start")
        flight_price_api = Variable.get("flight_price_api")
        
        results = []
        failed = [] # Failed actor
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
                datasetId = api_actor_run(run_input, flight_price_api)
                if datasetId:
                    results.append([depAirportCode, depCountryCode, currencyCode, nowSearchDate, datasetId])
                    
            time.sleep(180)
            
        logging.info("api call success")
        return results
        
    @task
    def data_to_raw(dataset_list, current_date):
        """
        load api data to raw layer
        """
        logging.info("start load to raw layer")
        time.sleep(100) # to wait dataset completed
        
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
        return dataset_list

                
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
        
        return dataset_list
            
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
                WHERE extractedDate <= '{current_date}';
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
        return current_date
        
        
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
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise e
        
        try:
            cur.execute("BEGIN;")
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
    search_date = {f'date_{days}': f'{{{{ macros.ds_add(ds, {days}) }}}}' for days in range(1, 31)}
    
    # departures = get_high_frequency_airports()
    # api_call_task = api_call(departures, search_date)
    
    # data_to_raw_task = data_to_raw(api_call_task, current_date)
    # raw_to_stage_task = raw_to_stage(data_to_raw_task, current_date)
    dataset_list = [['MFM', 'MO', 'MOP', '2024-08-09', 'RHPp9CRvmyfwPnTn0'], ['MFM', 'MO', 'MOP', '2024-08-10', 'CcvgtFZ4dV4lZxhmH'], ['MFM', 'MO', 'MOP', '2024-08-11', 'O1NX7H0syLuZNm65F'], ['MFM', 'MO', 'MOP', '2024-08-12', 'J8IaQGRlwwa6brG1H'], ['MFM', 'MO', 'MOP', '2024-08-13', 'FmOblVVNYFZ3SdnDc'], ['MFM', 'MO', 'MOP', '2024-08-14', 'OUoXTLcZFwb08BbNv'], ['MFM', 'MO', 'MOP', '2024-08-15', '8t3qiBJMP3LH3Sy5y'], ['MFM', 'MO', 'MOP', '2024-08-16', 'HSeGSxnocHzYnIDIL'], ['MFM', 'MO', 'MOP', '2024-08-17', 'hvEHqMIUpV9HiDUjm'], ['MFM', 'MO', 'MOP', '2024-08-18', '1AAS5iaVH1UxPpGOY'], ['MFM', 'MO', 'MOP', '2024-08-19', 'hI1qAflx73QnHp0Eu'], ['MFM', 'MO', 'MOP', '2024-08-20', '0r0qBdWsPk7bTy7MG'], ['MFM', 'MO', 'MOP', '2024-08-21', 'pxH6KkzFhve1yUHZJ'], ['MFM', 'MO', 'MOP', '2024-08-22', 'JP1XD8TVhZtydsoDA'], ['MFM', 'MO', 'MOP', '2024-08-23', 'dOXhWB7FQFE12NBfJ'], ['MFM', 'MO', 'MOP', '2024-08-24', 'w6dtV2xXHx9689b36'], ['MFM', 'MO', 'MOP', '2024-08-25', 'lvlzKvJvbgxBe6IHC'], ['MFM', 'MO', 'MOP', '2024-08-26', 'kjBBe1M1uTrKgj4Pg'], ['MFM', 'MO', 'MOP', '2024-08-27', 'jkXcCa71F1lVl2OmG'], ['MFM', 'MO', 'MOP', '2024-08-28', 'R4lLS7lyHUX0Rzbzd'], ['MFM', 'MO', 'MOP', '2024-08-29', 'j9Iy5KNLRvzskZ2Bc'], ['MFM', 'MO', 'MOP', '2024-08-30', '7rE23bb8lo9W9e3nN'], ['SEA', 'US', 'USD', '2024-08-01', 'd6taxHVubvb4c8An2'], ['SEA', 'US', 'USD', '2024-08-02', 'efHZEHclMfWel1cVp'], ['SEA', 'US', 'USD', '2024-08-03', 'ElsCrM8v4gdVByeWn'], ['SEA', 'US', 'USD', '2024-08-04', 'kjNNDbixhmt7AqUcZ'], ['SEA', 'US', 'USD', '2024-08-05', '4BXzVGlt9rrm5fShP'], ['SEA', 'US', 'USD', '2024-08-06', 'bhNdoHcjM4CDcG2Hs'], ['SEA', 'US', 'USD', '2024-08-07', 'wYw7GL4cX7gzT9f0Z'], ['SEA', 'US', 'USD', '2024-08-08', 'tiTVyATF3Cx2lWgK0'], ['SEA', 'US', 'USD', '2024-08-09', '6TcR8nudrBmUCYdIH'], ['SEA', 'US', 'USD', '2024-08-10', 'ehhEmaW9yTEt1nAvR'], ['SEA', 'US', 'USD', '2024-08-11', 'FlwYl6uFDK3PEz9r9'], ['SEA', 'US', 'USD', '2024-08-12', 'uu2uYhXfCDj6dnVPm'], ['SEA', 'US', 'USD', '2024-08-13', 'TWMq4dYWaLUPOaQL4'], ['SEA', 'US', 'USD', '2024-08-14', '2Ez7jate1HnjPk1TE'], ['SEA', 'US', 'USD', '2024-08-15', 'IvQYT2icq1baHZOEl'], ['SEA', 'US', 'USD', '2024-08-16', 'GQUe7TxcMX79spOBr'], ['SEA', 'US', 'USD', '2024-08-17', '2QQXcxk91KULzwHQU'], ['SEA', 'US', 'USD', '2024-08-18', 'XtA4KkX8ldnE0aCXU'], ['SEA', 'US', 'USD', '2024-08-19', 'hfvgzqbQrUW5cwJg6'], ['SEA', 'US', 'USD', '2024-08-20', 'MDabgklMLKSMNxKQ8'], ['SEA', 'US', 'USD', '2024-08-21', 'QW78KtbyRllf6671K'], ['SEA', 'US', 'USD', '2024-08-22', 'NJceyMnosowyOzBJg'], ['SEA', 'US', 'USD', '2024-08-23', 'Xd2hG6KzfFlMivxQz'], ['SEA', 'US', 'USD', '2024-08-24', 'AQ54Ja55NHdwlwnHv'], ['SEA', 'US', 'USD', '2024-08-25', 'Bj74kamRqNvXIopco'], ['SEA', 'US', 'USD', '2024-08-26', 'lpWHDoKcT6O11oVeZ'], ['SEA', 'US', 'USD', '2024-08-27', 'SxPNKf7coaiQhq9v0'], ['SEA', 'US', 'USD', '2024-08-28', 'Ow61LwfXWlEfKgvpp'], ['SEA', 'US', 'USD', '2024-08-29', 'ZtUmTpJ7nOPETSEyX'], ['SEA', 'US', 'USD', '2024-08-30', 'tS3IMc1sklL3eEWpV'], ['CNX', 'TH', 'THB', '2024-08-01', 'rSrR3Io6stly9MpaL'], ['CNX', 'TH', 'THB', '2024-08-02', 'zuWSg2swfFCv5rX1W'], ['CNX', 'TH', 'THB', '2024-08-03', '0qefMZ3nvjgmegqEC'], ['CNX', 'TH', 'THB', '2024-08-04', 'vxdwlwdrHetvSgUPD'], ['CNX', 'TH', 'THB', '2024-08-05', 've5MbK7raKAk390E2'], ['CNX', 'TH', 'THB', '2024-08-06', 'KHHF0kdnBxcOzdEOO'], ['CNX', 'TH', 'THB', '2024-08-07', 'dVFEOWxkRfOuoLU65'], ['CNX', 'TH', 'THB', '2024-08-08', 'kh9yciaDjWqbibeLz'], ['CNX', 'TH', 'THB', '2024-08-09', 'haQ9Xg9MPfs7fcw0I'], ['CNX', 'TH', 'THB', '2024-08-10', 'GAiKO23K20AljsaQI'], ['CNX', 'TH', 'THB', '2024-08-11', 'VWmUpolEB96odimM9'], ['CNX', 'TH', 'THB', '2024-08-12', 'FrN63yKt4datTXXC0'], ['CNX', 'TH', 'THB', '2024-08-13', 'hM4g6e6ObMZJDY3lS'], ['CNX', 'TH', 'THB', '2024-08-14', 'Ga6ysuGaHDQ7HPn4u'], ['CNX', 'TH', 'THB', '2024-08-15', 'ZSHMHaDcIXWBIIabi'], ['CNX', 'TH', 'THB', '2024-08-16', 'A7csJpH6dV832wtPQ'], ['CNX', 'TH', 'THB', '2024-08-17', 'PguMTOtfA0DyhwyjB'], ['CNX', 'TH', 'THB', '2024-08-18', 'Ztj8dtvWEGguc3bK4'], ['CNX', 'TH', 'THB', '2024-08-19', 'B3zbKrEcLyxNFdYiH'], ['CNX', 'TH', 'THB', '2024-08-20', 'OgjRhj0KjLyWz30be'], ['CNX', 'TH', 'THB', '2024-08-21', 'TgbLO2aIgySQonOIT'], ['CNX', 'TH', 'THB', '2024-08-22', 'SdIA5Vk8bRZmWtA4A'], ['CNX', 'TH', 'THB', '2024-08-23', 'wcLhsutrxOUtftaRe'], ['CNX', 'TH', 'THB', '2024-08-24', 'Pqm70bfM4H0geKYmN'], ['CNX', 'TH', 'THB', '2024-08-25', 'C7loo9ZnleALEYYJ1'], ['CNX', 'TH', 'THB', '2024-08-26', 'ideZDZgWgiD8iobh0'], ['CNX', 'TH', 'THB', '2024-08-27', '2eNAGOqGwz31QbZU7'], ['CNX', 'TH', 'THB', '2024-08-28', 'wVVFLLCOs8cCZACtF'], ['CNX', 'TH', 'THB', '2024-08-29', 'LOaxrRiRXhi23xY3s'], ['CNX', 'TH', 'THB', '2024-08-30', '4Vo0W9yuFLKapj10x'], ['TSN', 'CN', 'CNY', '2024-08-01', '7y0ddMTD0b4jMZS5X'], ['TSN', 'CN', 'CNY', '2024-08-02', 'Q1Fx8g2hXT6Yumm01'], ['TSN', 'CN', 'CNY', '2024-08-03', '3zouLe0RqIjD9TkO2'], ['TSN', 'CN', 'CNY', '2024-08-04', 'jeq1V2lVWMBtwwCLV'], ['TSN', 'CN', 'CNY', '2024-08-05', 'Da9I0LdXbalsUUlPQ'], ['TSN', 'CN', 'CNY', '2024-08-06', 'LjqUVKN02nDKQ2dA8'], ['TSN', 'CN', 'CNY', '2024-08-07', 'zHnUKMDO1bwVGpUTZ'], ['TSN', 'CN', 'CNY', '2024-08-08', 'jTk0RUndZA1clzfKc'], ['TSN', 'CN', 'CNY', '2024-08-09', 'k3Yi9RoPHjvy7ojPN'], ['TSN', 'CN', 'CNY', '2024-08-10', 'hUcbmjboFC72frf7Y'], ['TSN', 'CN', 'CNY', '2024-08-11', 'AFmMovVUIHzHeOzPm'], ['TSN', 'CN', 'CNY', '2024-08-12', 'THtv0bBVYLXTY5UhS'], ['TSN', 'CN', 'CNY', '2024-08-13', 'uHl2sepq4JXrW8Ef8'], ['TSN', 'CN', 'CNY', '2024-08-14', '4JOVajOyyVXxmlryM'], ['TSN', 'CN', 'CNY', '2024-08-15', 'jSnvZx6l2evkYTWWM'], ['TSN', 'CN', 'CNY', '2024-08-16', 'UFvh53yx6YKhIZhEk'], ['TSN', 'CN', 'CNY', '2024-08-17', 'rbDZdIF0XNsLBtSMD'], ['TSN', 'CN', 'CNY', '2024-08-18', 'bPyYzus1fZSvL8enw'], ['TSN', 'CN', 'CNY', '2024-08-19', 'ozbx3OXoOfGUdaOnz'], ['TSN', 'CN', 'CNY', '2024-08-20', 'RjvYogXz90TTXFD5Z'], ['TSN', 'CN', 'CNY', '2024-08-21', 'WM1zc4yjmmMKlDdmm'], ['TSN', 'CN', 'CNY', '2024-08-22', '9hSdC5667vgKVeVjS'], ['TSN', 'CN', 'CNY', '2024-08-23', 'DjLt9DsSZCz10nniI'], ['TSN', 'CN', 'CNY', '2024-08-24', '3htknqWI0RzGk4F7d'], ['TSN', 'CN', 'CNY', '2024-08-25', '8Q2DmrvHWrH79g3bB'], ['TSN', 'CN', 'CNY', '2024-08-26', '3oFQya930yE7ylaZ6'], ['TSN', 'CN', 'CNY', '2024-08-27', '1Zodh2f6DG3wSkY15'], ['TSN', 'CN', 'CNY', '2024-08-28', 'eWKY3JLP5in5AUw6u'], ['TSN', 'CN', 'CNY', '2024-08-29', 'lZpXAQiE0AjvBiylU'], ['TSN', 'CN', 'CNY', '2024-08-30', 'vtPnJlshQy5QqznTO'], ['HNL', 'US', 'USD', '2024-08-01', 'NZ6lsMZd9ZXr91MUf'], ['HNL', 'US', 'USD', '2024-08-02', 'RdpzVoYdP2AfozdL0'], ['HNL', 'US', 'USD', '2024-08-03', 'zalPcDTNOIAbyAJtg'], ['HNL', 'US', 'USD', '2024-08-04', 'eJMEUoZ7AkGSlXZt7'], ['HNL', 'US', 'USD', '2024-08-05', 'haSFxF6Nv50FaZgJE'], ['HNL', 'US', 'USD', '2024-08-06', 'rN8GCnoN94wl6FsSr'], ['HNL', 'US', 'USD', '2024-08-07', 'FjBhMmED6jNyRRqIa'], ['HNL', 'US', 'USD', '2024-08-08', '3udiHnhpMV2Dz3PVf'], ['HNL', 'US', 'USD', '2024-08-09', 'qTNjqt4OKq7Oostf7'], ['HNL', 'US', 'USD', '2024-08-10', 'NIPKhntciGOZApd9y'], ['HNL', 'US', 'USD', '2024-08-11', 'CXX34yUgk45w2nvLR'], ['HNL', 'US', 'USD', '2024-08-12', 'zLpG2DFPAv8Offcpg'], ['HNL', 'US', 'USD', '2024-08-13', 'QTzJcgF47O46CMhsP'], ['HNL', 'US', 'USD', '2024-08-14', '4Y3OS83eDQmPom8vl'], ['HNL', 'US', 'USD', '2024-08-15', 'ECg8P9c9zEyeJRQnW'], ['HNL', 'US', 'USD', '2024-08-16', 'myhbx4Ef5yXdDgtF6'], ['HNL', 'US', 'USD', '2024-08-17', 'MW0jeWdGaBJpkMQMV'], ['HNL', 'US', 'USD', '2024-08-18', 'LOquVdWymNRY4CCP3'], ['HNL', 'US', 'USD', '2024-08-19', 'gemx4t3RtP1b7GSDh'], ['HNL', 'US', 'USD', '2024-08-20', '8rQzXuuZCscscpffM'], ['HNL', 'US', 'USD', '2024-08-21', 'zQwJCp97lMZBgCLpV'], ['HNL', 'US', 'USD', '2024-08-22', 'LS2y93eJq2FOQ4sNp'], ['HNL', 'US', 'USD', '2024-08-23', 'S1gWbVqhk0CrHSQcO'], ['HNL', 'US', 'USD', '2024-08-24', '3pCKLjkzjEeGa65kf'], ['HNL', 'US', 'USD', '2024-08-25', '1IrVsX1JrpfaDgJLl'], ['HNL', 'US', 'USD', '2024-08-26', 'KCeEe380tEr1ku7QC'], ['HNL', 'US', 'USD', '2024-08-27', '1nxzPKuBnAvY1NEjm'], ['HNL', 'US', 'USD', '2024-08-28', 'mDFEG3Hk1YmG0PcE0'], ['HNL', 'US', 'USD', '2024-08-29', 'j7BpgRfpCQrn6d9i5'], ['HNL', 'US', 'USD', '2024-08-30', 'oHC0MB3dR061pIrQQ'], ['HGH', 'CN', 'CNY', '2024-08-01', '0Q14EfP44cP2SFTkj'], ['HGH', 'CN', 'CNY', '2024-08-02', 'uG6gIws7Ih4PssACc'], ['HGH', 'CN', 'CNY', '2024-08-03', 'mvORrYUKdY3kPiZHQ'], ['HGH', 'CN', 'CNY', '2024-08-04', 'xncfqBgvtZABs6k9o'], ['HGH', 'CN', 'CNY', '2024-08-05', 'tEMXw4LjVosxudunL'], ['HGH', 'CN', 'CNY', '2024-08-06', '4KsYkhuiUg2KME0NW'], ['HGH', 'CN', 'CNY', '2024-08-07', 'KZmCa37NCkuAPNumY'], ['HGH', 'CN', 'CNY', '2024-08-08', 'gJOjelRCTG6hdLkQN'], ['HGH', 'CN', 'CNY', '2024-08-09', 'JJM9EcX6K8rYoQgNC'], ['HGH', 'CN', 'CNY', '2024-08-10', 'PtMfZYlmeJ3Ukt2Pk'], ['HGH', 'CN', 'CNY', '2024-08-11', 'ieOd4j68HRXEc6zeQ'], ['HGH', 'CN', 'CNY', '2024-08-12', '2HJPnqnE0l986oF3J'], ['HGH', 'CN', 'CNY', '2024-08-13', 'vSmY0Bzcfg7uPeWqs'], ['HGH', 'CN', 'CNY', '2024-08-14', '7LH4pt0sECofjdVh2'], ['HGH', 'CN', 'CNY', '2024-08-15', 'HqEpAMdyFdH65EqJK'], ['HGH', 'CN', 'CNY', '2024-08-16', '4I5e4O3dzmNOf3Bh2'], ['HGH', 'CN', 'CNY', '2024-08-17', 'OVZ6b2Qh4GLRmXvCb'], ['HGH', 'CN', 'CNY', '2024-08-18', 'Afb0bjlpPYyhPabfe'], ['HGH', 'CN', 'CNY', '2024-08-19', 'jbjhRyj3qz7B5h4gH'], ['HGH', 'CN', 'CNY', '2024-08-20', 'UT2pADJQaKJwar8aR'], ['HGH', 'CN', 'CNY', '2024-08-21', '8bwAjEhKda6lbMLCJ'], ['HGH', 'CN', 'CNY', '2024-08-22', 'j4MOs21gYqafsiO5s'], ['HGH', 'CN', 'CNY', '2024-08-23', 'FuuSgVo04ykUPX6LP'], ['HGH', 'CN', 'CNY', '2024-08-24', 'cA0f8ZcmWPU4zXgQP'], ['HGH', 'CN', 'CNY', '2024-08-25', '24bTGspkABMJGCv0c'], ['HGH', 'CN', 'CNY', '2024-08-26', '93ITa9Z7kBvaw2J2P'], ['HGH', 'CN', 'CNY', '2024-08-27', 'IEIVPIzCGDWZcHh16'], ['HGH', 'CN', 'CNY', '2024-08-28', '2m3Yq1PPjr8IHUD66'], ['HGH', 'CN', 'CNY', '2024-08-29', 'H66mXy67Wr0j2ptWm'], ['HGH', 'CN', 'CNY', '2024-08-30', 'T1MhpND7pTJhAmFTA'], ['CAN', 'CN', 'CNY', '2024-08-01', 'KB0XvxwadekpIqoBz'], ['CAN', 'CN', 'CNY', '2024-08-02', 'KmAlpIO04agi8nyNb'], ['CAN', 'CN', 'CNY', '2024-08-03', 'f47dBzIWciEb0Illz'], ['CAN', 'CN', 'CNY', '2024-08-04', 'ugCP8eNGILsZzHrsc'], ['CAN', 'CN', 'CNY', '2024-08-05', 'gmA6EBb0GRP0BXEZ3'], ['CAN', 'CN', 'CNY', '2024-08-06', 'ygkdZUsyyVfSL7uUD'], ['CAN', 'CN', 'CNY', '2024-08-07', 'XmomAwmO0h9ue1VgS'], ['CAN', 'CN', 'CNY', '2024-08-08', 'rer0IaBEPsbvVpSdJ'], ['CAN', 'CN', 'CNY', '2024-08-09', '2BeuBgbUgdMQS36uX'], ['CAN', 'CN', 'CNY', '2024-08-10', 'iqpFw9Id2YZ2JI9Vq'], ['CAN', 'CN', 'CNY', '2024-08-11', 'TaHNXZHywEyPVUv81'], ['CAN', 'CN', 'CNY', '2024-08-12', 'VA7pwALpHaWoLVJnl'], ['CAN', 'CN', 'CNY', '2024-08-13', 'Xuw7MvZyPtagHRNmh'], ['CAN', 'CN', 'CNY', '2024-08-14', 'x7NUtAQ7jE2addp3E'], ['CAN', 'CN', 'CNY', '2024-08-15', 'XjiDLpgKJlM5XoBxG'], ['CAN', 'CN', 'CNY', '2024-08-16', 'WppMv3NKRnjzj5yT1'], ['CAN', 'CN', 'CNY', '2024-08-17', '1Xsb1RnyVy4wcfY9R'], ['CAN', 'CN', 'CNY', '2024-08-18', 'hscMcJWuyQTceCQG0'], ['CAN', 'CN', 'CNY', '2024-08-19', '096Nqelaz16KNfhUG'], ['CAN', 'CN', 'CNY', '2024-08-20', 'dSIyeJizXXuFH2COS'], ['CAN', 'CN', 'CNY', '2024-08-21', 'JbMXc6iP2mfhSwQ7I'], ['CAN', 'CN', 'CNY', '2024-08-22', 'd9CUbT1F7JFgMotZ2'], ['CAN', 'CN', 'CNY', '2024-08-23', 'TXsmaGXiHjzw9kjwN'], ['CAN', 'CN', 'CNY', '2024-08-24', 'dqSal5S8n4J2tGctr'], ['CAN', 'CN', 'CNY', '2024-08-25', 't0p3dxEJJ1swWr7yO'], ['CAN', 'CN', 'CNY', '2024-08-26', 'jKhnSYk5hSRnOevEr'], ['CAN', 'CN', 'CNY', '2024-08-27', 'gwa5k4YWAmsxLauMi'], ['CAN', 'CN', 'CNY', '2024-08-28', 'xXetMfRrLqrv0103J'], ['CAN', 'CN', 'CNY', '2024-08-29', 'aBdpGtWeFFZkp4nkW'], ['CAN', 'CN', 'CNY', '2024-08-30', 'VSIDDv4QDsKFq0gpp'], ['MNL', 'PH', 'PHP', '2024-08-01', 'MgR22OGYIeEFzQv3T'], ['MNL', 'PH', 'PHP', '2024-08-02', 'ub1Noke59px3EbnC1'], ['MNL', 'PH', 'PHP', '2024-08-03', 'bubXU9uLk9hqi3fPM'], ['MNL', 'PH', 'PHP', '2024-08-04', 'Qa7gggSmkbTmc3aHb'], ['MNL', 'PH', 'PHP', '2024-08-05', 'k5ysFivxhE2vcTVhs'], ['MNL', 'PH', 'PHP', '2024-08-06', 'nr3gK8PoN8IaU4Mdc'], ['MNL', 'PH', 'PHP', '2024-08-07', '1aNKFwitq0tmRz347'], ['MNL', 'PH', 'PHP', '2024-08-08', 'SgucDaRwoEmqzjdiZ'], ['MNL', 'PH', 'PHP', '2024-08-09', 'grCIL3QmhqR8R5u4h'], ['MNL', 'PH', 'PHP', '2024-08-10', 'h1cBhcneePyunpIsI'], ['MNL', 'PH', 'PHP', '2024-08-11', '80Yp5rUw7tSqSflSG'], ['MNL', 'PH', 'PHP', '2024-08-12', 'tRuaVhF95l0J3emit'], ['MNL', 'PH', 'PHP', '2024-08-13', 'wsPqVG75CVkODUmSN'], ['MNL', 'PH', 'PHP', '2024-08-14', '56wj9l0YPJbyJpoDO'], ['MNL', 'PH', 'PHP', '2024-08-15', 'BLLxfwDa799BqUhkt'], ['MNL', 'PH', 'PHP', '2024-08-16', 'YyWUW0ip2VzuLKLXU'], ['MNL', 'PH', 'PHP', '2024-08-17', 'O7mywcisCvRnTRIaW'], ['MNL', 'PH', 'PHP', '2024-08-18', 'TjA5sffBgmx6cAeo5'], ['MNL', 'PH', 'PHP', '2024-08-19', 'epPfeZoCjhyhylKNy'], ['MNL', 'PH', 'PHP', '2024-08-20', 'HoNLSD3SfQZYruRV5'], ['MNL', 'PH', 'PHP', '2024-08-21', '8Q99oxdcOJHRg4ucI'], ['MNL', 'PH', 'PHP', '2024-08-22', 'cOaGOSmKsVOiV8sbM'], ['MNL', 'PH', 'PHP', '2024-08-23', 'IT71Ou1w715Vw51Zt'], ['MNL', 'PH', 'PHP', '2024-08-24', '9aBstCuhwOA8QhZWL'], ['MNL', 'PH', 'PHP', '2024-08-25', '0dD68CZ1pAVWyKglZ'], ['MNL', 'PH', 'PHP', '2024-08-26', 'dsAt1QwczDjq39Ci6'], ['MNL', 'PH', 'PHP', '2024-08-27', 'OEa9MRsTUEr5hmbTB'], ['MNL', 'PH', 'PHP', '2024-08-28', 'ldv8q3loYR2GOZdZ3'], ['MNL', 'PH', 'PHP', '2024-08-29', '8RtPHo2nbzBB2gTmn'], ['MNL', 'PH', 'PHP', '2024-08-30', 'SsJ52uTYCyHR3ut4y'], ['CGK', 'ID', 'IDR', '2024-08-01', 'PhTqa59tk3VWkyKNT'], ['CGK', 'ID', 'IDR', '2024-08-02', 'tHbIAWHuT7JH0AxGE'], ['CGK', 'ID', 'IDR', '2024-08-03', 'entf9zJgPuJAsxRrj'], ['CGK', 'ID', 'IDR', '2024-08-04', 'v4luDU8hrE4ewva3E'], ['CGK', 'ID', 'IDR', '2024-08-05', '7um79a4WArRldjrKi'], ['CGK', 'ID', 'IDR', '2024-08-06', 'TZsmoGMW6SYJqrvhh'], ['CGK', 'ID', 'IDR', '2024-08-07', 'r3czowqEsa14lqOOu'], ['CGK', 'ID', 'IDR', '2024-08-08', 'rrpRoN4iDYllbuUFG'], ['CGK', 'ID', 'IDR', '2024-08-09', 'G2V0BgSOQNGzLGXSN'], ['CGK', 'ID', 'IDR', '2024-08-10', 'VOL8DGFN5qqr1P9pI'], ['CGK', 'ID', 'IDR', '2024-08-11', 'SKg6rdtttKq7JBjsN'], ['CGK', 'ID', 'IDR', '2024-08-12', 'Es48YbK9ueDssgpca'], ['CGK', 'ID', 'IDR', '2024-08-13', 'a6NY4Gygrm5lebo1n'], ['CGK', 'ID', 'IDR', '2024-08-14', 'Mga0MUj25gXAEGuSl'], ['CGK', 'ID', 'IDR', '2024-08-15', 'Mk0xWwsq2iJDos8OD'], ['CGK', 'ID', 'IDR', '2024-08-16', 'McHuVQtSo2mm74Cff'], ['CGK', 'ID', 'IDR', '2024-08-17', 'nXihKqqpFjhgtxxXZ'], ['CGK', 'ID', 'IDR', '2024-08-18', 'PMyKV6ihis2uIoHu9'], ['CGK', 'ID', 'IDR', '2024-08-19', 'eAtBMbU4k5TCeoyGu'], ['CGK', 'ID', 'IDR', '2024-08-20', 'S0fo9gYhRFvNaCzbI'], ['CGK', 'ID', 'IDR', '2024-08-21', 'YNxRTWQ7RrYkR2UAy'], ['CGK', 'ID', 'IDR', '2024-08-22', 'z7BTU1jROhsXfPQwT'], ['CGK', 'ID', 'IDR', '2024-08-23', '81KrUJhrPSlMvTfIQ'], ['CGK', 'ID', 'IDR', '2024-08-24', '3QkxlSQUfH6CAXxTd'], ['CGK', 'ID', 'IDR', '2024-08-25', 'mPsfao7Uewhhtr9b6'], ['CGK', 'ID', 'IDR', '2024-08-26', 'PxGzwYlicn9Kdt19a'], ['CGK', 'ID', 'IDR', '2024-08-27', 'Qz7TVdlwzcOZ9afyz'], ['CGK', 'ID', 'IDR', '2024-08-28', 'prUFnHRbLz8ZsxbCV'], ['CGK', 'ID', 'IDR', '2024-08-29', 'thh8schv8CeeYvo5z'], ['CGK', 'ID', 'IDR', '2024-08-30', '51rVTQlpQk6JdYbdC'], ['OKA', 'JP', 'JPY', '2024-08-01', 'G9PUQmR5K2YomiLv4'], ['OKA', 'JP', 'JPY', '2024-08-02', '7VIcfgBUu5UH8R6Ga'], ['OKA', 'JP', 'JPY', '2024-08-03', '2CyTLFdpFPWtcA8jL'], ['OKA', 'JP', 'JPY', '2024-08-04', 'DA6YTufFiRqzHO22O'], ['OKA', 'JP', 'JPY', '2024-08-05', 'bnvjjJRW33An95Fey'], ['OKA', 'JP', 'JPY', '2024-08-06', 'Ht7xuU7l93mArjIJ7'], ['OKA', 'JP', 'JPY', '2024-08-07', '1KAhBo3uBsg2xhBet'], ['OKA', 'JP', 'JPY', '2024-08-08', 'CiXBswjJ2HEjJiOKf'], ['OKA', 'JP', 'JPY', '2024-08-09', '7nSlA6cgahbUdOps5'], ['OKA', 'JP', 'JPY', '2024-08-10', 'XupQ4O0xyKo99iQxn'], ['OKA', 'JP', 'JPY', '2024-08-11', 'nwvRGrp8Z9TsPNyhL'], ['OKA', 'JP', 'JPY', '2024-08-12', 'GKstjhs70eduXmAs3'], ['OKA', 'JP', 'JPY', '2024-08-13', '5klzVeZ9PeGI2s4IE'], ['OKA', 'JP', 'JPY', '2024-08-14', 'phnanYu3AU5VtDyo7'], ['OKA', 'JP', 'JPY', '2024-08-15', 'zCDlUWyzSxsEOSZn2'], ['OKA', 'JP', 'JPY', '2024-08-16', 'bypIQ9u4MdYuVL9EE'], ['OKA', 'JP', 'JPY', '2024-08-17', 'o76tYqjBkzFAXf1qL'], ['OKA', 'JP', 'JPY', '2024-08-18', 'Tl5VrHeXKa8b7hbiL'], ['OKA', 'JP', 'JPY', '2024-08-19', '3ZCOGlKT0TnHlg52C'], ['OKA', 'JP', 'JPY', '2024-08-20', 'yAfLl12whNO8MQcit'], ['OKA', 'JP', 'JPY', '2024-08-21', 'ZgzRjVhWaePE5WYLB'], ['OKA', 'JP', 'JPY', '2024-08-22', 'DvYnNg3z4GSWsrE60'], ['OKA', 'JP', 'JPY', '2024-08-23', 'wCUANWRU7clUcYG7p'], ['OKA', 'JP', 'JPY', '2024-08-24', 'd6x09CPM6zJrkabRa'], ['OKA', 'JP', 'JPY', '2024-08-25', 'AFVPySdJp9XsLScTk'], ['OKA', 'JP', 'JPY', '2024-08-26', 'FlUNJ6BP03fQQ16AM'], ['OKA', 'JP', 'JPY', '2024-08-27', 'ThWK2Za51Yu10t2sU'], ['OKA', 'JP', 'JPY', '2024-08-28', 'hAKKoKOHmRX6BNera'], ['OKA', 'JP', 'JPY', '2024-08-29', 'MJl0brImOQ7ZMu6ai'], ['OKA', 'JP', 'JPY', '2024-08-30', 'DHZyYfHNcMDHdPvm2'], ['SFO', 'US', 'USD', '2024-08-01', 'JlcSnKbqBmSMo92Wg'], ['SFO', 'US', 'USD', '2024-08-02', 'vGu2bcEun7wTgmNoS'], ['SFO', 'US', 'USD', '2024-08-03', 'G5OMbWa4mk8zdnujR'], ['SFO', 'US', 'USD', '2024-08-04', '7ZKnz0xK8N1fdqK6m'], ['SFO', 'US', 'USD', '2024-08-05', 'iGKBf4dYyQvfnWekY'], ['SFO', 'US', 'USD', '2024-08-06', 'gxngr3TJnOXjBDXQI'], ['SFO', 'US', 'USD', '2024-08-07', 'RfmNdDgM7cCVOlQGK'], ['SFO', 'US', 'USD', '2024-08-08', 'cjcVg67hTcawzlR1o'], ['SFO', 'US', 'USD', '2024-08-09', 'QupAL4WKqt6fde0MO'], ['SFO', 'US', 'USD', '2024-08-10', '7OWSp7mHjnjH0d5zf'], ['SFO', 'US', 'USD', '2024-08-11', '3BXFtpbEkThb1Abwa'], ['SFO', 'US', 'USD', '2024-08-12', '2ggz3KSgXN5iJBkQn'], ['SFO', 'US', 'USD', '2024-08-13', 'pB13DyuYzQCJPutCS'], ['SFO', 'US', 'USD', '2024-08-14', '1d8NZ1tazk2voUOeQ'], ['SFO', 'US', 'USD', '2024-08-15', 'jJhy7qCn7hu7K2kF1'], ['SFO', 'US', 'USD', '2024-08-16', 'Dird0EXImlGi4ifUt'], ['SFO', 'US', 'USD', '2024-08-17', 'mcIWqd9hiu5IUnxAl'], ['SFO', 'US', 'USD', '2024-08-18', 'OIFkGnTUf6QGGGLdv'], ['SFO', 'US', 'USD', '2024-08-19', 'QFrYVNLyBzWFravjB'], ['SFO', 'US', 'USD', '2024-08-20', 'bwNFi7y1zOGi0GFhA'], ['SFO', 'US', 'USD', '2024-08-21', 'RbkYLXT0E99RiZBpp'], ['SFO', 'US', 'USD', '2024-08-22', 'Z5uc5gsMA5JF8lxao'], ['SFO', 'US', 'USD', '2024-08-23', 'J2Tdixo1kSSMiAm48'], ['SFO', 'US', 'USD', '2024-08-24', 'YpIJG1RCnqtidChFt'], ['SFO', 'US', 'USD', '2024-08-25', 'NiHtx5IFYHvsOwgEn'], ['SFO', 'US', 'USD', '2024-08-26', 'nkKbvwdZ3rX7ByefL'], ['SFO', 'US', 'USD', '2024-08-27', 'HUOkKrK3l2vZHOSK6'], ['SFO', 'US', 'USD', '2024-08-28', 'HM9xMfILJ24rFnvqA'], ['SFO', 'US', 'USD', '2024-08-29', 'VrjvbIsZDjuW1kvmf'], ['SFO', 'US', 'USD', '2024-08-30', 'VHD10uSt5wI71KgZY'], ['DLC', 'CN', 'CNY', '2024-08-01', 'WzSj9rnekqDbvLvRM'], ['DLC', 'CN', 'CNY', '2024-08-02', 'd51yerRMudTIxqP5m'], ['DLC', 'CN', 'CNY', '2024-08-03', 'CQtPNM90GvcUZrRBO'], ['DLC', 'CN', 'CNY', '2024-08-04', 'XSQdPbur8nccO1vzB'], ['DLC', 'CN', 'CNY', '2024-08-05', 'nFASFZ4IJ67hKDF0n'], ['DLC', 'CN', 'CNY', '2024-08-06', 'SYaQ7Q954hdhw9xHD'], ['DLC', 'CN', 'CNY', '2024-08-07', 'ds4oyWJQPZ6jRyrDM'], ['DLC', 'CN', 'CNY', '2024-08-08', 'bYSIvY0giCS4zIsse'], ['DLC', 'CN', 'CNY', '2024-08-09', '2FzQIcCOt3OsYzo9Y'], ['DLC', 'CN', 'CNY', '2024-08-10', 'J4dS3hI5kxMQHhu8F'], ['DLC', 'CN', 'CNY', '2024-08-11', 'G6uZ5bJAfrd1BTvLW'], ['DLC', 'CN', 'CNY', '2024-08-12', '31krRtcS5kJxNNhb4'], ['DLC', 'CN', 'CNY', '2024-08-13', 'HgK4mPZ7s9gOpFT8K'], ['DLC', 'CN', 'CNY', '2024-08-14', 'hxvtC25s9YLc7VTrS'], ['DLC', 'CN', 'CNY', '2024-08-15', 'bZnpfmPj6Isvl4Bh7'], ['DLC', 'CN', 'CNY', '2024-08-16', 'X8N5lUedaO6O2aekX'], ['DLC', 'CN', 'CNY', '2024-08-17', 'M0pHKW2SSnwUqLpOB'], ['DLC', 'CN', 'CNY', '2024-08-18', 'renz0PQgoTePOgNxb'], ['DLC', 'CN', 'CNY', '2024-08-19', '15ih25vTqs6NJFu3s'], ['DLC', 'CN', 'CNY', '2024-08-20', 'lsfUcjaUniGg0rSpY'], ['DLC', 'CN', 'CNY', '2024-08-21', '5hyQTteQUwIh2aUI4'], ['DLC', 'CN', 'CNY', '2024-08-22', 'UHnzs5ns8upfQcziU'], ['DLC', 'CN', 'CNY', '2024-08-23', '9bgVaDnCXfzzfieFb'], ['DLC', 'CN', 'CNY', '2024-08-24', 'pVIcUsOoJdnrbO6z9'], ['DLC', 'CN', 'CNY', '2024-08-25', 'yIppsXPjwTizdVYIZ'], ['DLC', 'CN', 'CNY', '2024-08-26', 'R67Medga4WbhyYivh'], ['DLC', 'CN', 'CNY', '2024-08-27', 'VEE63SGA7mNhY8f2I'], ['DLC', 'CN', 'CNY', '2024-08-28', 'fhvg6TsheVdElz6YU'], ['DLC', 'CN', 'CNY', '2024-08-29', 'GvZcwpIwnr6My3N8g'], ['DLC', 'CN', 'CNY', '2024-08-30', 'q06XmcjApXvAUYqW5']]

    update_redshift_task = update_redshift(dataset_list, current_date)
    
    unload_s3_task = unload_redshift_to_s3(current_date)
    update_rds_task = update_rds(unload_s3_task)
    # departures >> api_call_task >> data_to_raw_task >> raw_to_stage_task >> update_redshift_task >> unload_s3_task >> update_rds_task
    update_redshift_task >> unload_s3_task >> update_rds_task
    
dag=dag()