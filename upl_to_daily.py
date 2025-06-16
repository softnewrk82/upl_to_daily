from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

import uuid

import warnings
warnings.simplefilter("ignore")

import pandas as pd
import numpy as np 

import datetime

import pendulum

date_now = datetime.datetime.now().date()

from functools import lru_cache

from sqlalchemy import create_engine 

# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.operators.postgres import PostgresOperator

import importlib

import modules.api_info
importlib.reload(modules.api_info)

# ________________________________________

from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encrypt_var_db_name_for_upl

from modules.api_info import var_encrypt_var_db_for_upl_schema
from modules.api_info import var_encrypt_var_db_for_upl_schema_inter
from modules.api_info import var_encrypt_var_db_for_upl_schema_service_toolkit
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encrypt_var_db_for_upl_schema_to
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")

var_db_name_for_upl = f_decrypt(var_encrypt_var_db_name_for_upl, load_key_external()).decode("utf-8")
var_db_for_upl_schema = f_decrypt(var_encrypt_var_db_for_upl_schema, load_key_external()).decode("utf-8")
var_db_for_upl_schema_inter = f_decrypt(var_encrypt_var_db_for_upl_schema_inter, load_key_external()).decode("utf-8")
var_db_for_upl_schema_service_toolkit = f_decrypt(var_encrypt_var_db_for_upl_schema_service_toolkit, load_key_external()).decode("utf-8")
var_db_for_upl_schema_to = f_decrypt(var_encrypt_var_db_for_upl_schema_to, load_key_external()).decode("utf-8")


var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")

API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")


local_tz = pendulum.timezone("Europe/Moscow")


default_arguments = {
    'owner': 'evgenijgrinev',
}


with DAG(
    'upl_to_daily',
    schedule_interval='0 19 * * *',
    # schedule_interval='@once',
    catchup=False,
    default_args=default_arguments,
    start_date=pendulum.datetime(2024,7,1, tz=local_tz),
) as dag:

    
    def py_upl_script():


    python_upl_script = PythonOperator(
        task_id='python_upl_script',
        python_callable=py_upl_script
    )


python_upl_script