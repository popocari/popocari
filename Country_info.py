from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract_transform():
    response = requests.get('https://restcountries.com/v3/all')
    countries = response.json()
    records = []
    for country in countries:
        name = country['name']['official']
        population = country['population']
        area = country['area']

        records.append([name, population, area])
    return records


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Redshift_connection()   
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};") 
        cur.execute(f"""
                    CREATE TABLE {schema}.{table} (
                        name varchar(256),
                        population int,
                        area float
                    );""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            name = r[0]
            population = r[1]
            area = r[2]
            print(name, "-", population, "-", area)
            sql = f"INSERT INTO {schema}.name_gender VALUES ('{name}', '{population}', '{area}')"
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   
    logging.info("load done")


with DAG(
    dag_id='Country_info',
    start_date=datetime(2024, 5, 20),  # 날짜가 미래인 경우 실행이 안됨
    schedule='30 6 * * 6',  
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    schema = 'zxcvb1703'  
    table = 'country_info'
    results = extract_transform()
    load(schema, table, results)