from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_user_session_channel():


    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"""CREATE OR REPLACE TABLE dev.raw_data.user_session_channel (
        userId int NOT NULL,
        sessionId varchar(32) PRIMARY KEY,
        channel varchar(32) DEFAULT 'direct')"""
        logging.info(sql)
        cur.execute(sql)
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise

@task
def create_session_timestamp():


    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"""CREATE OR REPLACE TABLE dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp)"""
        logging.info(sql)
        cur.execute(sql)
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise

@task
def create_blob_stage():


    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"""CREATE OR REPLACE STAGE dev.raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"')"""
        logging.info(sql)
        cur.execute(sql)
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise

@task
def copy_into_session_timestamp():


    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"""COPY INTO dev.raw_data.session_timestamp
    FROM @dev.raw_data.blob_stage/session_timestamp.csv"""
        logging.info(sql)
        cur.execute(sql)
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise

@task
def copy_into_user_session_channel():


    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"""COPY INTO dev.raw_data.user_session_channel
    FROM @dev.raw_data.blob_stage/user_session_channel.csv"""
        logging.info(sql)
        cur.execute(sql)
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise

with DAG(
    dag_id = 'SessionToSnowflake',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:
	cur = return_snowflake_conn()

	# Define task dependencies
	create_user_session_channel() >> create_session_timestamp() >> create_blob_stage() >> copy_into_user_session_channel() >> copy_into_session_timestamp()

