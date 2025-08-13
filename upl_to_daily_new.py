from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

import uuid

import requests
import json
import xmltodict

import warnings
warnings.simplefilter("ignore")

import pandas as pd
import numpy as np 

import datetime
from dateutil.relativedelta import relativedelta

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
from modules.api_info import var_encrypt_var_to_docs_upl
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
var_to_docs_upl = f_decrypt(var_encrypt_var_to_docs_upl, load_key_external()).decode("utf-8")


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


        def to_coll_sell_new(date_from, date_to, var_name_date_for_upl):            
            
                # ________________________________________________________________________________
                # lst_doc_add_f_date_from = []
                # lst_doc_add_f_date_to = []
                lst_doc_add_f_address = []
                lst_doc_add_f_sn_kkt = []
                lst_doc_add_f_new = []
                lst_doc_add_f_model = []
                
                lst_doc_author_id = []
                lst_doc_author = []
                lst_doc_date = []
                lst_doc_at_created = []
                lst_doc_base_date = []
                lst_doc_base_id = []
                lst_doc_base_num = []
                lst_doc_base_type = []
                lst_doc_base_sum = []
                lst_doc_add_f_q_kkt = []
                lst_doc_add_f_item = []
                lst_doc_add_f_send_edo = []
                lst_doc_add_f_pay_freq = []
                lst_doc_add_f_product = []
                lst_doc_add_f_tariff = []
                lst_doc_add_f_pay_term = []
                lst_doc_add_f_term_agreement = []
                lst_doc_id = []
                lst_doc_counterparty_inn = []
                lst_doc_counterparty_full_name = []
                lst_doc_full_name = []
                lst_doc_provider_inn = []
                lst_doc_provider_full_name = []
                lst_doc_number = []
                lst_doc_assigned_manager_id = []
                lst_doc_assigned_manager = []
                lst_doc_department_id = []
                lst_doc_department = []
                lst_doc_ext_conducted = []
                lst_doc_regulations_id = []
                lst_doc_regulations = []
                lst_doc_condition_code = []
                lst_doc_condition_name = []
                lst_doc_sum = []
                lst_doc_type = []
                lst_doc_deleted = []
                # ________________________________________________________________________________   






                
            
                url = url_sbis
                
                method = "СБИС.Аутентифицировать"
                params = {
                    "Параметр": {
                        "Логин": API_sbis,
                        "Пароль": API_sbis_pass
                    }
                
                }
                parameters = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": 0
                }
                
                response = requests.post(url, json=parameters)
                response.encoding = 'utf-8'
                
                str_to_dict = json.loads(response.text)
                access_token = str_to_dict["result"]
                # print("access_token:", access_token)
                
                headers = {
                "X-SBISSessionID": access_token,
                "Content-Type": "application/json",
                }  
                
                var_status_has_more = "Да"
                i_page = 0
                
                while var_status_has_more == "Да":
                   
                    parameters_real = {
                    "jsonrpc": "2.0",
                    "method": "СБИС.СписокДокументов",
                    "params": {
                        "Фильтр": {
                        "ДатаС": date_from,
                        "ДатаПо": date_to,
                        "Тип": "ДоговорИсх",
                        "Регламент": {
                            "Название": "Договор на ТО НОВЫЙ"
                        },
                        "Навигация": {
                            "Страница": i_page
                        }
                        }
                    },
                    "id": 0
                    }
                    
                    url_real = url_sbis_unloading
                    
                    response_points = requests.post(url_real, json=parameters_real, headers=headers)
                
                    str_to_dict_points_main = json.loads(response_points.text)
                
                    # _________________________________________________________________________
                    
            
                    response = requests.post(url, json=parameters)
                    response.encoding = 'utf-8'
                    
                    str_to_dict = json.loads(response.text)
                    access_token = str_to_dict["result"]
                    # print("access_token:", access_token)
                    
                    headers = {
                    "X-SBISSessionID": access_token,
                    "Content-Type": "application/json",
                    }  
                    
                    # _________________________________________________________________________
            
                    print('i_page:', i_page)
            
                    for i in range(len(str_to_dict_points_main["result"]["Документ"])):
                        # ________________________________________________________________________________
                        # if str_to_dict_points_main["result"]["Документ"][i]["Состояние"]["Название"] == 'Выполнение завершено успешно':
            
                            # print(str_to_dict_points_main["result"]["Документ"])
                            var_link = str_to_dict_points_main["result"]["Документ"][i]["Идентификатор"]
                            
                            parameters_real = {
                               "jsonrpc": "2.0",
                               "method": "СБИС.ПрочитатьДокумент",
                               "params": {
                                  "Документ": {
                                     "Идентификатор": var_link,
                                     "ДопПоля": "ДополнительныеПоля"
                                  }
                               },
                               "id": 0
                            }
                            # ________________________________________________________________________________
                                    
                            url_real = url_sbis_unloading
                            
                            response_points = requests.post(url_real, json=parameters_real, headers=headers)
                            str_to_dict_points = json.loads(response_points.text)
            
                
            
                            # ________________________________________________________________________________
                            # ________________________________________________________________________________
                            # ________________________________________________________________________________
                 
                            doc_author_id = str_to_dict_points["result"]["Автор"]["Идентификатор"]
                            doc_author = ' '.join([str_to_dict_points["result"]["Автор"]["Фамилия"], str_to_dict_points["result"]["Автор"]["Имя"], str_to_dict_points["result"]["Автор"]["Отчество"]])
                            doc_date = str_to_dict_points["result"]["Дата"]
                            doc_at_created = str_to_dict_points["result"]["ДатаВремяСоздания"]
                            
                            try:
                                doc_base = str_to_dict_points["result"]["ДокументОснование"]
                                for j in range(len(doc_base)): 
                                    
                                    if 'отгр' in re.findall('отгр', doc_base[j]["Документ"]["Тип"].lower()):
                                        try:
                                            doc_base_date = doc_base[j]["Документ"]["Дата"]
                                        except:
                                            doc_base_date = ''
                                        
                                        try:
                                            doc_base_id = doc_base[j]["Документ"]["Идентификатор"]
                                        except:
                                            doc_base_id = ''
                                        
                                        try:
                                            doc_base_num = doc_base[j]["Документ"]["Номер"]
                                        except:
                                            doc_base_num = ''
                                        
                                        try:
                                            doc_base_type = doc_base[j]["Документ"]["Тип"]
                                        except:
                                            doc_base_type = ''
                                        
                                        try:
                                            doc_base_sum = doc_base[j]["Сумма"]
                                        except:
                                            doc_base_sum = ''
                                            
                                    else:
                                        doc_base_date = ''
                                        doc_base_id = ''
                                        doc_base_num = ''
                                        doc_base_type = ''
                                        doc_base_sum = ''
                            except:
                                doc_base_date = ''
                                doc_base_id = ''
                                doc_base_num = ''
                                doc_base_type = ''
                                doc_base_sum = ''   
            
                            # ________________________________________________________________________________________
                            try:    
                                doc_add_f_q_kkt = add_f["КоличествоККТ"]
                            except: 
                                doc_add_f_q_kkt = ''
                            try:
                                doc_add_f_item = add_f["Оборудование"]
                            except: 
                                doc_add_f_item = ''
                            try:
                                doc_add_f_send_edo = add_f["ОтправитьЭДО"]
                            except:
                                doc_add_f_send_edo = ''
                            try:
                                doc_add_f_pay_freq = add_f["ПериодичностьОплаты"]
                            except: 
                                doc_add_f_pay_freq = ''
                            try:
                                doc_add_f_product = add_f["Продукт"]
                            except: 
                                doc_add_f_product = ''
                            try:
                                doc_add_f_tariff = add_f["Тариф"]
                            except:
                                doc_add_f_tariff = ''
                            try:
                                doc_add_f_pay_term = add_f["УсловиеОплаты"]
                            except: 
                                doc_add_f_pay_term = ''
                            try: 
                                doc_add_f_term_agreement = add_f["УсловияДоговора"]
                            except: 
                                doc_add_f_term_agreement = ''
                            # ________________________________________________________________________________________
                            doc_id = str_to_dict_points["result"]["Идентификатор"]
                            
                            try:
                                doc_counterparty_inn = str_to_dict_points["result"]["Контрагент"]["СвФЛ"]["ИНН"]
                                doc_counterparty_full_name = str_to_dict_points["result"]["Контрагент"]["СвФЛ"]["НазваниеПолное"]
                            except:
                                doc_counterparty_inn = str_to_dict_points["result"]["Контрагент"]["СвЮЛ"]["ИНН"]
                                doc_counterparty_full_name = str_to_dict_points["result"]["Контрагент"]["СвЮЛ"]["НазваниеПолное"]
                            
                            doc_full_name = str_to_dict_points["result"]["Название"]
                            
                            try:
                                doc_provider_inn = str_to_dict_points["result"]["НашаОрганизация"]["СвФЛ"]["ИНН"]
                                doc_provider_full_name = str_to_dict_points["result"]["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"]
                            except:
                                doc_provider_inn = str_to_dict_points["result"]["НашаОрганизация"]["СвЮЛ"]["ИНН"]
                                doc_provider_full_name = str_to_dict_points["result"]["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"]
                            
                            doc_number = str_to_dict_points["result"]["Номер"]
                            doc_assigned_manager_id = str_to_dict_points["result"]["Ответственный"]["Идентификатор"]
                            doc_assigned_manager = ' '.join([str_to_dict_points["result"]["Ответственный"]["Фамилия"], str_to_dict_points["result"]["Ответственный"]["Имя"], str_to_dict_points["result"]["Ответственный"]["Отчество"]])
                            try:     
                                doc_department_id = str_to_dict_points["result"]["Подразделение"]["Идентификатор"]
                            except: 
                                doc_department_id = ''
                            try:
                                doc_department = str_to_dict_points["result"]["Подразделение"]["Название"]
                            except:
                                doc_department = ''
                            doc_ext_conducted = str_to_dict_points["result"]["Расширение"]["Проведен"]
                            doc_regulations_id = str_to_dict_points["result"]["Регламент"]["Идентификатор"]
                            doc_regulations = str_to_dict_points["result"]["Регламент"]["Название"]
                            doc_condition_code = str_to_dict_points["result"]["Состояние"]["Код"]
                            doc_condition_name = str_to_dict_points["result"]["Состояние"]["Название"]
                            doc_sum = str_to_dict_points["result"]["Сумма"]
                            doc_type = str_to_dict_points["result"]["Тип"]
                            doc_deleted = str_to_dict_points["result"]["Удален"]
                            # ________________________________________________________________________________________
            
                            
                            # ________________________________________________________________________________________
                            # ________________________________________________________________________________________
                            # ________________________________________________________________________________________
                            add_f = str_to_dict_points["result"]["ДополнительныеПоля"]['Определение условий договора']
                            for k in add_f.keys():
                                if 'ккт ' in k.lower():
                                    if len(add_f[k]) != 0:
                                        # add_f[k]
                                        # break
            
                                        # try:
                                        #     doc_add_f_date_from = add_f[k]["ДействуетС"]
                                        # except:
                                        #     doc_add_f_date_from = ''
                                        # try:
                                        #     doc_add_f_date_to = add_f[k]["ДействуетПо"]
                                        # except:
                                        #     doc_add_f_date_to = ''
                                        
                                        try:
                                            doc_add_f_address = add_f[k]["Адрес" + re.findall('\d+', k)[0]]
                                        except:
                                            doc_add_f_address = ''
                                        try:
                                            doc_add_f_sn_kkt = add_f[k]["ЗаводскойККТ" + re.findall('\d+', k)[0]]
                                        except: 
                                            doc_add_f_sn_kkt = ''
                                        try:
                                            doc_add_f_new = add_f[k]["ККТНовое"]
                                        except:
                                            doc_add_f_new = ''
                                        try:
                                            doc_add_f_model = add_f[k]["Модель" + re.findall('\d+', k)[0]]
                                        except:
                                            doc_add_f_model = ''
                                            
                                        # print('doc_add_f_date_from:', doc_add_f_date_from)
                                        # print('doc_add_f_date_to:', doc_add_f_date_to)
                                        print('doc_add_f_address:', doc_add_f_address)
                                        print('doc_add_f_sn_kkt:', doc_add_f_sn_kkt)
                                        print('doc_add_f_new:', doc_add_f_new)
                                        print('doc_add_f_model:', doc_add_f_model)                           
                            
                                        print('doc_author_id:', doc_author_id)
                                        print('doc_author:', doc_author)
                                        print('doc_date:', doc_date)
                                        print('doc_at_created:', doc_at_created)
                                        print('doc_base_date:', doc_base_date)
                                        print('doc_base_id:', doc_base_id)
                                        print('doc_base_num:', doc_base_num)
                                        print('doc_base_type:', doc_base_type)
                                        print('doc_base_sum:', doc_base_sum)
                                        
                                        print('doc_add_f_q_kkt:', doc_add_f_q_kkt)
                                        print('doc_add_f_item:', doc_add_f_item)
                                        print('doc_add_f_send_edo:', doc_add_f_send_edo)
                                        print('doc_add_f_pay_freq:', doc_add_f_pay_freq)
                                        print('doc_add_f_product:', doc_add_f_product)
                                        print('doc_add_f_tariff:', doc_add_f_tariff)
                                        print('doc_add_f_pay_term:', doc_add_f_pay_term)
                                        print('doc_add_f_term_agreement:', doc_add_f_term_agreement)
                                        
                                        print('doc_id:', doc_id)
                                        print('doc_counterparty_inn:', doc_counterparty_inn)
                                        print('doc_counterparty_full_name:', doc_counterparty_full_name)
                                        print('doc_full_name:', doc_full_name)
                                        print('doc_provider_inn:', doc_provider_inn)
                                        print('doc_provider_full_name:', doc_provider_full_name)
                                        print('doc_number:', doc_number)
                                        print('doc_assigned_manager_id:', doc_assigned_manager_id)
                                        print('doc_assigned_manager:', doc_assigned_manager)
                                        print('doc_department_id:', doc_department_id)
                                        print('doc_department:', doc_department)
                                        print('doc_ext_conducted:', doc_ext_conducted)
                                        print('doc_regulations_id:', doc_regulations_id)
                                        print('doc_regulations:', doc_regulations)
                                        print('doc_condition_code:', doc_condition_code)
                                        print('doc_condition_name:', doc_condition_name)
                                        print('doc_sum:', doc_sum)
                                        print('doc_type:', doc_type)
                                        print('doc_deleted:', doc_deleted)
                                        print('________________________________________________________________________________')
                                        
                                        # lst_doc_add_f_date_from.append(doc_add_f_date_from)
                                        # lst_doc_add_f_date_to.append(doc_add_f_date_to)
                                        lst_doc_add_f_address.append(doc_add_f_address)
                                        lst_doc_add_f_sn_kkt.append(doc_add_f_sn_kkt)
                                        lst_doc_add_f_new.append(doc_add_f_new)
                                        lst_doc_add_f_model.append(doc_add_f_model)
                                        
                                        lst_doc_author_id.append(doc_author_id)
                                        lst_doc_author.append(doc_author)
                                        lst_doc_date.append(doc_date)
                                        lst_doc_at_created.append(doc_at_created)
                                        lst_doc_base_date.append(doc_base_date)
                                        lst_doc_base_id.append(doc_base_id)
                                        lst_doc_base_num.append(doc_base_num)
                                        lst_doc_base_type.append(doc_base_type)
                                        lst_doc_base_sum.append(doc_base_sum)
                                        
                                        lst_doc_add_f_q_kkt.append(doc_add_f_q_kkt)
                                        lst_doc_add_f_item.append(doc_add_f_item)
                                        lst_doc_add_f_send_edo.append(doc_add_f_send_edo)
                                        lst_doc_add_f_pay_freq.append(doc_add_f_pay_freq)
                                        lst_doc_add_f_product.append(doc_add_f_product)
                                        lst_doc_add_f_tariff.append(doc_add_f_tariff)
                                        lst_doc_add_f_pay_term.append(doc_add_f_pay_term)
                                        lst_doc_add_f_term_agreement.append(doc_add_f_term_agreement)
                                        
                                        lst_doc_id.append(doc_id)
                                        lst_doc_counterparty_inn.append(doc_counterparty_inn)
                                        lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                                        lst_doc_full_name.append(doc_full_name)
                                        lst_doc_provider_inn.append(doc_provider_inn)
                                        lst_doc_provider_full_name.append(doc_provider_full_name)
                                        lst_doc_number.append(doc_number)
                                        lst_doc_assigned_manager_id.append(doc_assigned_manager_id)
                                        lst_doc_assigned_manager.append(doc_assigned_manager)
                                        lst_doc_department_id.append(doc_department_id)
                                        lst_doc_department.append(doc_department)
                                        lst_doc_ext_conducted.append(doc_ext_conducted)
                                        lst_doc_regulations_id.append(doc_regulations_id)
                                        lst_doc_regulations.append(doc_regulations)
                                        lst_doc_condition_code.append(doc_condition_code)
                                        lst_doc_condition_name.append(doc_condition_name)
                                        lst_doc_sum.append(doc_sum)
                                        lst_doc_type.append(doc_type)
                                        lst_doc_deleted.append(doc_deleted)            
            
                    if var_status_has_more == "Нет":
                        break
                    elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
                        i_page += 1
                    else:
                        pass
                    var_status_has_more = str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"]
                        
                df_to = pd.DataFrame(columns=[
                    # 'doc_add_f_date_from',
                    # 'doc_add_f_date_to',
                    'doc_add_f_address',
                    'doc_add_f_sn_kkt',
                    'doc_add_f_new',
                    'doc_add_f_model',
                    
                    'doc_author_id',
                    'doc_author',
                    'doc_date',
                    'doc_at_created',
                    'doc_base_date',
                    'doc_base_id',
                    'doc_base_num',
                    'doc_base_type',
                    'doc_base_sum',
                    'doc_add_f_q_kkt',
                    'doc_add_f_item',
                    'doc_add_f_send_edo',
                    'doc_add_f_pay_freq',
                    'doc_add_f_product',
                    'doc_add_f_tariff',
                    'doc_add_f_pay_term',
                    'doc_add_f_term_agreement',
                    'doc_id',
                    'doc_counterparty_inn',
                    'doc_counterparty_full_name',
                    'doc_full_name',
                    'doc_provider_inn',
                    'doc_provider_full_name',
                    'doc_number',
                    'doc_assigned_manager_id',
                    'doc_assigned_manager',
                    'doc_department_id',
                    'doc_department',
                    'doc_ext_conducted',
                    'doc_regulations_id',
                    'doc_regulations',
                    'doc_condition_code',
                    'doc_condition_name',
                    'doc_sum',
                    'doc_type',
                    'doc_deleted',
                ], data= zip(
                    # lst_doc_add_f_date_from,
                    # lst_doc_add_f_date_to,
                    lst_doc_add_f_address,
                    lst_doc_add_f_sn_kkt,
                    lst_doc_add_f_new,
                    lst_doc_add_f_model,
                    
                    lst_doc_author_id,
                    lst_doc_author,
                    lst_doc_date,
                    lst_doc_at_created,
                    lst_doc_base_date,
                    lst_doc_base_id,
                    lst_doc_base_num,
                    lst_doc_base_type,
                    lst_doc_base_sum,
                    lst_doc_add_f_q_kkt,
                    lst_doc_add_f_item,
                    lst_doc_add_f_send_edo,
                    lst_doc_add_f_pay_freq,
                    lst_doc_add_f_product,
                    lst_doc_add_f_tariff,
                    lst_doc_add_f_pay_term,
                    lst_doc_add_f_term_agreement,
                    lst_doc_id,
                    lst_doc_counterparty_inn,
                    lst_doc_counterparty_full_name,
                    lst_doc_full_name,
                    lst_doc_provider_inn,
                    lst_doc_provider_full_name,
                    lst_doc_number,
                    lst_doc_assigned_manager_id,
                    lst_doc_assigned_manager,
                    lst_doc_department_id,
                    lst_doc_department,
                    lst_doc_ext_conducted,
                    lst_doc_regulations_id,
                    lst_doc_regulations,
                    lst_doc_condition_code,
                    lst_doc_condition_name,
                    lst_doc_sum,
                    lst_doc_type,
                    lst_doc_deleted,
                ))
            
                my_conn = create_engine(f"postgresql+psycopg2://da:qa123@10.82.2.30:5432/{var_db_name}")
                try: 
                    my_conn.connect()
                    print('connect')
                    my_conn = my_conn.connect()
                    df_to.to_sql(name=f"df_to_new_{var_name_date_for_upl}", schema=f"{var_to_docs_upl}" , con=my_conn, if_exists='replace')
                    print('success!')
                    my_conn.close()
                except:
                    print('failed')     






            

        # ________________________________________________________
        report_date = datetime.datetime.now().date()
        var_year = (report_date - relativedelta(months=2)).year
        var_month = (report_date - relativedelta(months=2)).month
        year_range = list(range(var_year, report_date.year+1))
        # ________________________________________________________
            
        for i_year in year_range:

            if len(year_range) != 1:
                if i_year != report_date.year:
                    month_range = list(range(var_month,13))
                    for i_month in month_range:
                        # ____________________
                        print(i_year, i_month)
                        # ____________________
                        var_name_date_for_upl = datetime.datetime(i_year,i_month,1).date().strftime('%Y%m%d')
                        start_date = datetime.datetime(i_year, i_month, 1).date().strftime('%d.%m.%Y')
                        end_date = (datetime.datetime(i_year, i_month, 1).date() + relativedelta(months=1, days=-1)).strftime('%d.%m.%Y')
                        # print(var_name_date_for_upl, start_date, end_date)
                        to_coll_sell(start_date, end_date, var_name_date_for_upl)
                        # print(f"{var_name_date_for_upl} success")
                        # print("\n")
                        # print("________________________________________________________________________________________________")
                else:
                    month_range = list(range(1,13))
                    for i_month in month_range:
                        # ____________________
                        print(i_year, i_month)
                        # ____________________                
                        var_name_date_for_upl = datetime.datetime(i_year,i_month,1).date().strftime('%Y%m%d')
                        start_date = datetime.datetime(i_year, i_month, 1).date().strftime('%d.%m.%Y')
                        end_date = (datetime.datetime(i_year, i_month, 1).date() + relativedelta(months=1, days=-1)).strftime('%d.%m.%Y')
                        # print(var_name_date_for_upl, start_date, end_date)
                        to_coll_sell(start_date, end_date, var_name_date_for_upl)
                        # print(f"{var_name_date_for_upl} success")
                        # print("\n")
                        # print("________________________________________________________________________________________________")
                
            else:
                month_range = list(range(var_month,13))
                for i_month in month_range:
                    # ____________________
                    print(i_year, i_month)
                    # ____________________  
                    var_name_date_for_upl = datetime.datetime(i_year,i_month,1).date().strftime('%Y%m%d')
                    start_date = datetime.datetime(i_year, i_month, 1).date().strftime('%d.%m.%Y')
                    end_date = (datetime.datetime(i_year, i_month, 1).date() + relativedelta(months=1, days=-1)).strftime('%d.%m.%Y')
                    # print(var_name_date_for_upl, start_date, end_date)
                    to_coll_sell(start_date, end_date, var_name_date_for_upl)
                    # print(f"{var_name_date_for_upl} success")
                    # print("\n")
                    # print("________________________________________________________________________________________________")

    python_upl_script = PythonOperator(
        task_id='python_upl_script',
        python_callable=py_upl_script
    )


python_upl_script