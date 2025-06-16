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


        def to_coll_sell(date_from, date_to, var_name_date_for_upl):


            lst_doc_id = []
            lst_doc_type = []
            lst_doc_number = []
            lst_doc_full_name = []
            lst_doc_data_main = []
            lst_doc_at_created = []
            lst_doc_counterparty_inn = []
            lst_doc_counterparty_full_name = []
            lst_doc_provider_inn = []
            lst_doc_provider_full_name = []
            lst_doc_assigned_manager = []
            lst_doc_department = []
            lst_inside_doc_author = []
            lst_doc_inside_assigned_manager = []
            lst_inside_doc_type = []
            lst_inside_doc_item_full_doc_price = []
            lst_sn_cash_register = []
            lst_name_cash_register = []
            lst_doc_tariff = []
            lst_end_data_fn = []
            lst_quantity_of_cash_register = []

            lst_sale_number = []
            lst_sale_id = []
            lst_sale_type = []

            lst_adress = []
            lst_manufacture_year = []
            lst_cash_sn = []
            lst_model_cash = []
            lst_inside_doc_quantity_of_cash_register = []
            
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
                        "Название": "Договор на ТО"
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
                    # if     
                    # ________________________________________________________________________________
                    if str_to_dict_points_main["result"]["Документ"][i]["Состояние"]["Название"] == 'Выполнение завершено успешно':
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
                                
                        url_real = url_sbis_unloading
                        
                        response_points = requests.post(url_real, json=parameters_real, headers=headers)
                        str_to_dict_points = json.loads(response_points.text)
                        
                        # json_data_points = json.dumps(str_to_dict_points, ensure_ascii=False, indent=4).encode("utf8").decode()
                        
                        # with open("DICT_REALIZE.json", 'w') as json_file_points_o:
                        #     json_file_points_o.write(json_data_points)
                        

                        # print(var_link)



                        # for i_part in str_to_dict_points["result"]["ДокументОснование"]:
                        #     try:
                        #         if i_part["Документ"]["Тип"].lower() == 'докотгрисх':
                        #             # print(i_part["Документ"]["Номер"])
                        #             # print(i_part["Документ"]["Регламент"]["Идентификатор"])
                        #             # print(i_part["Документ"]["Тип"])
                
                        #             doc_sale_number = i_part["Документ"]["Номер"]
                        #             doc_sale_id = i_part["Документ"]["Регламент"]["Идентификатор"]
                        #             doc_sale_type = i_part["Документ"]["Тип"]
                        #         else:
                        #             doc_sale_number = ''
                        #             doc_sale_id = ''
                        #             doc_sale_type = ''
                        #     except:
                        #         doc_sale_number = ''
                        #         doc_sale_id = ''
                        #         doc_sale_type = ''  
                                

                        

                        doc_id = str_to_dict_points["result"]["Идентификатор"]
                        doc_type = str_to_dict_points["result"]["Регламент"]["Название"] 
                        doc_number = str_to_dict_points["result"]["Номер"]
                        doc_full_name = str_to_dict_points["result"]["Название"]
                        doc_data_main = str_to_dict_points["result"]["Дата"]
                        doc_at_created = str_to_dict_points["result"]["ДатаВремяСоздания"]
                        
                        try:
                            doc_counterparty_inn = str_to_dict_points["result"]["Контрагент"]["СвФЛ"]["ИНН"]
                            doc_counterparty_full_name = str_to_dict_points["result"]["Контрагент"]["СвФЛ"]["НазваниеПолное"]
                        except:
                            doc_counterparty_inn = str_to_dict_points["result"]["Контрагент"]["СвЮЛ"]["ИНН"]
                            doc_counterparty_full_name = str_to_dict_points["result"]["Контрагент"]["СвЮЛ"]["НазваниеПолное"]
                        
                        try:
                            doc_provider_inn = str_to_dict_points["result"]["НашаОрганизация"]["СвФЛ"]["ИНН"]
                            doc_provider_full_name = str_to_dict_points["result"]["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"]
                        except:
                            doc_provider_inn = str_to_dict_points["result"]["НашаОрганизация"]["СвЮЛ"]["ИНН"]
                            doc_provider_full_name = str_to_dict_points["result"]["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"]
                
                        try:
                            doc_assigned_manager = str_to_dict_points["result"]["Ответственный"]["Фамилия"] + " " + str_to_dict_points["result"]["Ответственный"]["Имя"] + " " + str_to_dict_points["result"]["Ответственный"]["Отчество"]
                        except: 
                            doc_assigned_manager = ''
                        try:
                            doc_department = str_to_dict_points["result"]["Подразделение"]["Название"]
                        except:
                            doc_department = ''

                        try:
                            inside_doc_author = str_to_dict_points["result"]["Автор"]["Фамилия"] + " " + str_to_dict_points["result"]["Автор"]["Имя"] + " " + str_to_dict_points["result"]["Автор"]["Отчество"]
                        except:
                            inside_doc_author = ''
                        
                        try:
                            doc_inside_assigned_manager = str_to_dict_points["result"]["ДополнительныеПоля"]["AAA"][0]["Фамилия"] + " " + str_to_dict_points["result"]["ДополнительныеПоля"]["AAA"][0]["Имя"] + " " + str_to_dict_points["result"]["ДополнительныеПоля"]["AAA"][0]["Отчество"] 
                        except:
                            doc_inside_assigned_manager  = ''

                        try:
                            inside_doc_type = str_to_dict_points["result"]["Тип"]
                        except:
                            inside_doc_type = ''

                        try:
                            inside_doc_item_full_doc_price = str_to_dict_points["result"]["Сумма"]
                        except:
                            inside_doc_item_full_doc_price = ''
                
                
                
                        try:
                            sn_cash_register = str_to_dict_points["result"]["ДополнительныеПоля"]["ЗН"]
                        except:
                            try:
                                sn_cash_register = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["ЗНКассы"]
                            except:
                                sn_cash_register = ''
                        
                        try:
                            name_cash_register = str_to_dict_points["result"]["ДополнительныеПоля"]["Касса"]
                        except:
                            try:
                                name_cash_register = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["ККТ"]
                            except:
                                name_cash_register = ''
                        
                        try: 
                            doc_tariff = str_to_dict_points["result"]["ДополнительныеПоля"]["Тариф"]
                        except:
                            doc_tariff = ''
                        
                        try:
                            end_data_fn = str_to_dict_points["result"]["ДополнительныеПоля"]["ФН"]
                        except:
                            try:
                                end_data_fn = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["СрокДействияФН"]
                            except:
                                end_data_fn = ''
                
                        try:
                            quantity_of_cash_register = str_to_dict_points["result"]["ДополнительныеПоля"]["количество"]
                        except:
                            quantity_of_cash_register = ''
                    
                        # ________________________________________________________________________________

                
                        # print(doc_id)
                        # print(doc_type)
                        # print(doc_number)
                        # print(doc_full_name)
                        # print(doc_data_main)
                        # print(doc_at_created)
                        # print(doc_counterparty_inn)
                        # print(doc_counterparty_full_name)
                        # print(doc_provider_inn)
                        # print(doc_provider_full_name)
                        
                        # print(doc_assigned_manager)
                        # print(doc_department)
                        # print(inside_doc_author)
                        # print(doc_inside_assigned_manager)
                        # print(inside_doc_type)
                        # print(inside_doc_item_full_doc_price)
                        
                        # print(sn_cash_register)
                        # print(name_cash_register)
                        # print(doc_tariff)
                        # print(end_data_fn)
                        # print(quantity_of_cash_register)
                        # print("________________________________________________________________________________")



                        
                        # try:
                        #     var_test = str_to_dict_points["result"]["ДокументОснование"]
                        #     # print(var_test)
                            
                        #     for i_part in str_to_dict_points["result"]["ДокументОснование"]:
                        #         try:
                        #             if i_part["Документ"]["Тип"].lower() == 'докотгрисх':
                        #                 # print(i_part["Документ"]["Номер"])
                        #                 # print(i_part["Документ"]["Регламент"]["Идентификатор"])
                        #                 # print(i_part["Документ"]["Тип"])
                    
                        #                 doc_sale_number = i_part["Документ"]["Номер"]
                        #                 doc_sale_id = i_part["Документ"]["Регламент"]["Идентификатор"]
                        #                 doc_sale_type = i_part["Документ"]["Тип"]
                        #             else:
                        #                 doc_sale_number = ''
                        #                 doc_sale_id = ''
                        #                 doc_sale_type = ''
                        #         except:
                        #             doc_sale_number = ''
                        #             doc_sale_id = ''
                        #             doc_sale_type = ''  
                        # except:
                        #     doc_sale_number = ''
                        #     doc_sale_id = ''
                        #     doc_sale_type = ''  


                        try:
        
                            attachments_id = {}
                            for i in range(len(str_to_dict_points["result"]["Вложение"])):
                                if str_to_dict_points["result"]["Вложение"][i]["Тип"].lower() in ("сведенияоккт"):
                                    attachments_id[str_to_dict_points["result"]["Вложение"][i]["Тип"].lower()] = str_to_dict_points["result"]["Вложение"][i]["Файл"]["Ссылка"]
                                else:
                                    pass


                            a = requests.get(attachments_id["сведенияоккт"], headers=headers)
                            a.encoding = "cp1251"
                            xml_a = xmltodict.parse(a.text) 
                            # print(doc_id)                                       
                            
                        except:
                                                
                            # print(doc_id)
                            # print('_________________________________________')
                            pass

                        
                        try:
                            if type(xml_a["Список"]["ККТ"]) == dict:
            
                                var_a = xml_a["Список"]["ККТ"]["@ЗаводскойНомер"].replace(' ', '').split(';')
                                
                                if len(var_a) == 1:
                                    try:
                                        adress = xml_a["Список"]["ККТ"]["@АдресУстановки"]
                                    except:
                                        adress = '' 
                        
                                    try:
                                        manufacture_year = xml_a["Список"]["ККТ"]["@ГодВыпуска"]
                                    except:
                                        manufacture_year = '' 
                    
                                    try:
                                        cash_sn = xml_a["Список"]["ККТ"]["@ЗаводскойНомер"]
                                        inside_cash_quantity = 1
                                    except:
                                        cash_sn = '' 
                                        inside_cash_quantity = ''
                    
                                    try:
                                        model_cash = xml_a["Список"]["ККТ"]["@Марка"]
                                    except:
                                        model_cash = '' 
                                        
                                    # _____________________________________
                                    
                                    lst_doc_id.append(doc_id)
                                    lst_doc_type.append(doc_type)
                                    lst_doc_number.append(doc_number)
                                    lst_doc_full_name.append(doc_full_name)
                                    lst_doc_data_main.append(doc_data_main)
                                    lst_doc_at_created.append(doc_at_created)
                                    lst_doc_counterparty_inn.append(doc_counterparty_inn)
                                    lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                                    lst_doc_provider_inn.append(doc_provider_inn)
                                    lst_doc_provider_full_name.append(doc_provider_full_name)     
                                    lst_doc_assigned_manager.append(doc_assigned_manager)
                                    lst_doc_department.append(doc_department)
                                    lst_inside_doc_author.append(inside_doc_author)
                                    lst_doc_inside_assigned_manager.append(doc_inside_assigned_manager)
                                    lst_inside_doc_type.append(inside_doc_type)
                                    lst_inside_doc_item_full_doc_price.append(inside_doc_item_full_doc_price)    
                                    lst_sn_cash_register.append(sn_cash_register)
                                    lst_name_cash_register.append(name_cash_register)
                                    lst_doc_tariff.append(doc_tariff)
                                    lst_end_data_fn.append(end_data_fn)
                                    lst_quantity_of_cash_register.append(quantity_of_cash_register)  
            
                                    # lst_sale_number.append(doc_sale_number)
                                    # lst_sale_id.append(doc_sale_id)
                                    # lst_sale_type.append(doc_sale_type)
                                    
                                    lst_adress.append(adress)
                                    lst_manufacture_year.append(manufacture_year)
                                    lst_cash_sn.append(cash_sn)
                                    lst_model_cash.append(model_cash)
                                    lst_inside_doc_quantity_of_cash_register.append(inside_cash_quantity)
                                    # _____________________________________
                                    # print(doc_number)
                                    # print(cash_sn)
                                
                                elif len(var_a) > 1:
            
                                    for i_sn in var_a:
            
                                        try:
                                            adress = xml_a["Список"]["ККТ"]["@АдресУстановки"]
                                        except:
                                            adress = '' 
                            
                                        try:
                                            manufacture_year = xml_a["Список"]["ККТ"]["@ГодВыпуска"]
                                        except:
                                            manufacture_year = '' 
                        
                                        try:
                                            cash_sn = i_sn
                                            inside_cash_quantity = 1
                                        except:
                                            cash_sn = '' 
                                            inside_cash_quantity = ''
                        
                                        try:
                                            model_cash = xml_a["Список"]["ККТ"]["@Марка"]
                                        except:
                                            model_cash = '' 
                                            
                                        # _____________________________________
                                        
                                        lst_doc_id.append(doc_id)
                                        lst_doc_type.append(doc_type)
                                        lst_doc_number.append(doc_number)
                                        lst_doc_full_name.append(doc_full_name)
                                        lst_doc_data_main.append(doc_data_main)
                                        lst_doc_at_created.append(doc_at_created)
                                        lst_doc_counterparty_inn.append(doc_counterparty_inn)
                                        lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                                        lst_doc_provider_inn.append(doc_provider_inn)
                                        lst_doc_provider_full_name.append(doc_provider_full_name)     
                                        lst_doc_assigned_manager.append(doc_assigned_manager)
                                        lst_doc_department.append(doc_department)
                                        lst_inside_doc_author.append(inside_doc_author)
                                        lst_doc_inside_assigned_manager.append(doc_inside_assigned_manager)
                                        lst_inside_doc_type.append(inside_doc_type)
                                        lst_inside_doc_item_full_doc_price.append(inside_doc_item_full_doc_price)    
                                        lst_sn_cash_register.append(sn_cash_register)
                                        lst_name_cash_register.append(name_cash_register)
                                        lst_doc_tariff.append(doc_tariff)
                                        lst_end_data_fn.append(end_data_fn)
                                        lst_quantity_of_cash_register.append(quantity_of_cash_register)  
            
                                        # lst_sale_number.append(doc_sale_number)
                                        # lst_sale_id.append(doc_sale_id)
                                        # lst_sale_type.append(doc_sale_type)
                                        
                                        lst_adress.append(adress)
                                        lst_manufacture_year.append(manufacture_year)
                                        lst_cash_sn.append(cash_sn)
                                        lst_model_cash.append(model_cash)
                                        lst_inside_doc_quantity_of_cash_register.append(inside_cash_quantity)
                                        # print(doc_number)
                                        # print(cash_sn)
                                
                            elif type(xml_a["Список"]["ККТ"]) == list:
                                for i_lst in xml_a["Список"]["ККТ"]:
                                    
                                    var_a = i_lst["@ЗаводскойНомер"].replace(' ', '').split(';')
            
                                    if len(var_a) == 1:
            
                                        try:
                                            adress = i_lst["@АдресУстановки"]
                                        except:
                                            adress = '' 
                            
                                        try:
                                            manufacture_year = i_lst["@ГодВыпуска"]
                                        except:
                                            manufacture_year = '' 
                        
                                        try:
                                            cash_sn = i_lst["@ЗаводскойНомер"]
                                            inside_cash_quantity = 1
                                        except:
                                            cash_sn = '' 
                                            inside_cash_quantity = ''
                        
                                        try:
                                            model_cash = i_lst["@Марка"]
                                        except:
                                            model_cash = '' 
            
                
            
                            
                                        lst_doc_id.append(doc_id)
                                        lst_doc_type.append(doc_type)
                                        lst_doc_number.append(doc_number)
                                        lst_doc_full_name.append(doc_full_name)
                                        lst_doc_data_main.append(doc_data_main)
                                        lst_doc_at_created.append(doc_at_created)
                                        lst_doc_counterparty_inn.append(doc_counterparty_inn)
                                        lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                                        lst_doc_provider_inn.append(doc_provider_inn)
                                        lst_doc_provider_full_name.append(doc_provider_full_name)     
                                        lst_doc_assigned_manager.append(doc_assigned_manager)
                                        lst_doc_department.append(doc_department)
                                        lst_inside_doc_author.append(inside_doc_author)
                                        lst_doc_inside_assigned_manager.append(doc_inside_assigned_manager)
                                        lst_inside_doc_type.append(inside_doc_type)
                                        lst_inside_doc_item_full_doc_price.append(inside_doc_item_full_doc_price)    
                                        lst_sn_cash_register.append(sn_cash_register)
                                        lst_name_cash_register.append(name_cash_register)
                                        lst_doc_tariff.append(doc_tariff)
                                        lst_end_data_fn.append(end_data_fn)
                                        lst_quantity_of_cash_register.append(quantity_of_cash_register)   
            
                                        # lst_sale_number.append(doc_sale_number)
                                        # lst_sale_id.append(doc_sale_id)
                                        # lst_sale_type.append(doc_sale_type)
                                        
                                        lst_adress.append(adress)
                                        lst_manufacture_year.append(manufacture_year)
                                        lst_cash_sn.append(cash_sn)
                                        lst_model_cash.append(model_cash)
                                        lst_inside_doc_quantity_of_cash_register.append(inside_cash_quantity)
                                        # _____________________________________
            
                                    elif len(var_a) > 1:
                                        for i_sn in var_a:
            
                                            try:
                                                adress = i_lst["@АдресУстановки"]
                                            except:
                                                adress = '' 
                                            
                                            try:
                                                manufacture_year = i_lst["@ГодВыпуска"]
                                            except:
                                                manufacture_year = '' 
                                            
                                            try:
                                                cash_sn = i_sn
                                                inside_cash_quantity = 1
                                            except:
                                                cash_sn = '' 
                                                inside_cash_quantity = ''
                                            
                                            try:
                                                model_cash = i_lst["@Марка"]
                                            except:
                                                model_cash = '' 
                                            
                                            
                                            
                                            
                                            lst_doc_id.append(doc_id)
                                            lst_doc_type.append(doc_type)
                                            lst_doc_number.append(doc_number)
                                            lst_doc_full_name.append(doc_full_name)
                                            lst_doc_data_main.append(doc_data_main)
                                            lst_doc_at_created.append(doc_at_created)
                                            lst_doc_counterparty_inn.append(doc_counterparty_inn)
                                            lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                                            lst_doc_provider_inn.append(doc_provider_inn)
                                            lst_doc_provider_full_name.append(doc_provider_full_name)     
                                            lst_doc_assigned_manager.append(doc_assigned_manager)
                                            lst_doc_department.append(doc_department)
                                            lst_inside_doc_author.append(inside_doc_author)
                                            lst_doc_inside_assigned_manager.append(doc_inside_assigned_manager)
                                            lst_inside_doc_type.append(inside_doc_type)
                                            lst_inside_doc_item_full_doc_price.append(inside_doc_item_full_doc_price)    
                                            lst_sn_cash_register.append(sn_cash_register)
                                            lst_name_cash_register.append(name_cash_register)
                                            lst_doc_tariff.append(doc_tariff)
                                            lst_end_data_fn.append(end_data_fn)
                                            lst_quantity_of_cash_register.append(quantity_of_cash_register)   
            
                                            # lst_sale_number.append(doc_sale_number)
                                            # lst_sale_id.append(doc_sale_id)
                                            # lst_sale_type.append(doc_sale_type)
                                            
                                            lst_adress.append(adress)
                                            lst_manufacture_year.append(manufacture_year)
                                            lst_cash_sn.append(cash_sn)
                                            lst_model_cash.append(model_cash)
                                            lst_inside_doc_quantity_of_cash_register.append(inside_cash_quantity)
                                            # _____________________________________

                        except:

                            # print('start_except')
                            # print('_______________________')
                            
                            # lst_doc_id.append(doc_id)
                            # lst_doc_type.append(doc_type)
                            # lst_doc_number.append(doc_number)
                            # lst_doc_full_name.append(doc_full_name)
                            # lst_doc_data_main.append(doc_data_main)
                            # lst_doc_at_created.append(doc_at_created)
                            # lst_doc_counterparty_inn.append(doc_counterparty_inn)
                            # lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                            # lst_doc_provider_inn.append(doc_provider_inn)
                            # lst_doc_provider_full_name.append(doc_provider_full_name)     
                            # lst_doc_assigned_manager.append(doc_assigned_manager)
                            # lst_doc_department.append(doc_department)
                            # lst_inside_doc_author.append(inside_doc_author)
                            # lst_doc_inside_assigned_manager.append(doc_inside_assigned_manager)
                            # lst_inside_doc_type.append(inside_doc_type)
                            # lst_inside_doc_item_full_doc_price.append(inside_doc_item_full_doc_price)    
                            # lst_sn_cash_register.append(sn_cash_register)
                            # lst_name_cash_register.append(name_cash_register)
                            # lst_doc_tariff.append(doc_tariff)
                            # lst_end_data_fn.append(end_data_fn)
                            # lst_quantity_of_cash_register.append(quantity_of_cash_register)   

                            # # lst_sale_number.append(doc_sale_number)
                            # # lst_sale_id.append(doc_sale_id)
                            # # lst_sale_type.append(doc_sale_type)



                            # str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["АдресКассы"]
                            # str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["ЗНКассы"]
                            # str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["ККТ"]
                            # str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["СрокДействияФН"]


                            var_a = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["ЗНКассы"].replace(' ', '').split(',')


                            
                            if len(var_a) == 1:

                                try:
                                    adress = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["АдресКассы"]
                                except:
                                    adress = '' 
                    
                                try:
                                    manufacture_year = ''
                                except:
                                    manufacture_year = '' 
                
                                try:
                                    cash_sn = var_a[0]
                                    inside_cash_quantity = 1
                                except:
                                    cash_sn = ''
                                    inside_cash_quantity = ''
                
                                try:
                                    model_cash = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["ККТ"]
                                except:
                                    model_cash = '' 

        

                    
                                lst_doc_id.append(doc_id)
                                lst_doc_type.append(doc_type)
                                lst_doc_number.append(doc_number)
                                lst_doc_full_name.append(doc_full_name)
                                lst_doc_data_main.append(doc_data_main)
                                lst_doc_at_created.append(doc_at_created)
                                lst_doc_counterparty_inn.append(doc_counterparty_inn)
                                lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                                lst_doc_provider_inn.append(doc_provider_inn)
                                lst_doc_provider_full_name.append(doc_provider_full_name)     
                                lst_doc_assigned_manager.append(doc_assigned_manager)
                                lst_doc_department.append(doc_department)
                                lst_inside_doc_author.append(inside_doc_author)
                                lst_doc_inside_assigned_manager.append(doc_inside_assigned_manager)
                                lst_inside_doc_type.append(inside_doc_type)
                                lst_inside_doc_item_full_doc_price.append(inside_doc_item_full_doc_price)    
                                lst_sn_cash_register.append(sn_cash_register)
                                lst_name_cash_register.append(name_cash_register)
                                lst_doc_tariff.append(doc_tariff)
                                lst_end_data_fn.append(end_data_fn)
                                lst_quantity_of_cash_register.append(quantity_of_cash_register)   

                                # lst_sale_number.append(doc_sale_number)
                                # lst_sale_id.append(doc_sale_id)
                                # lst_sale_type.append(doc_sale_type)
                                
                                lst_adress.append(adress)
                                lst_manufacture_year.append(manufacture_year)
                                lst_cash_sn.append(cash_sn)
                                lst_model_cash.append(model_cash)
                                lst_inside_doc_quantity_of_cash_register.append(inside_cash_quantity)
                            
                            else:

                                for i_sn in var_a:
                                
                                    try:
                                        adress = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["АдресКассы"]
                                    except:
                                        adress = ''
                
                                    try:
                                        cash_sn = i_sn
                                    except:
                                        cash_sn = ''
                                    
                                    try:
                                        model_cash = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["ККТ"]
                                    except:
                                        model_cash = ''
                                    
                                    try: 
                                        doc_tariff = str_to_dict_points["result"]["ДополнительныеПоля"]["Тариф"]
                                    except:
                                        doc_tariff = ''
                                    
                                    try:
                                        end_data_fn = str_to_dict_points["result"]["ДополнительныеПоля"]["Инф. из реализации"]["СрокДействияФН"]
                                    except:
                                        end_data_fn = ''
                            
                                    try:
                                        # inside_cash_quantity = str_to_dict_points["result"]["ДополнительныеПоля"]["количество"]
                                        inside_cash_quantity = 1
                                    except:
                                        inside_cash_quantity = ''
                                    
                                    manufacture_year = ''
            
                                    
                                    lst_doc_id.append(doc_id)
                                    lst_doc_type.append(doc_type)
                                    lst_doc_number.append(doc_number)
                                    lst_doc_full_name.append(doc_full_name)
                                    lst_doc_data_main.append(doc_data_main)
                                    lst_doc_at_created.append(doc_at_created)
                                    lst_doc_counterparty_inn.append(doc_counterparty_inn)
                                    lst_doc_counterparty_full_name.append(doc_counterparty_full_name)
                                    lst_doc_provider_inn.append(doc_provider_inn)
                                    lst_doc_provider_full_name.append(doc_provider_full_name)     
                                    lst_doc_assigned_manager.append(doc_assigned_manager)
                                    lst_doc_department.append(doc_department)
                                    lst_inside_doc_author.append(inside_doc_author)
                                    lst_doc_inside_assigned_manager.append(doc_inside_assigned_manager)
                                    lst_inside_doc_type.append(inside_doc_type)
                                    lst_inside_doc_item_full_doc_price.append(inside_doc_item_full_doc_price)    
                                    lst_sn_cash_register.append(sn_cash_register)
                                    lst_name_cash_register.append(name_cash_register)
                                    lst_doc_tariff.append(doc_tariff)
                                    lst_end_data_fn.append(end_data_fn)
                                    lst_quantity_of_cash_register.append(quantity_of_cash_register)   
                
                                    
                                    lst_adress.append(adress)
                                    lst_manufacture_year.append(manufacture_year)
                                    lst_cash_sn.append(cash_sn)
                                    lst_model_cash.append(model_cash)
                                    lst_inside_doc_quantity_of_cash_register.append(inside_cash_quantity)
                                    
                    
                if var_status_has_more == "Нет":
                    break
                elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
                    i_page += 1
                else:
                    pass
                var_status_has_more = str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"]
            
            df_to = pd.DataFrame(columns=[
                "doc_id",
                "doc_type",
                "doc_number",
                "doc_full_name",
                "doc_data_main",
                "doc_at_created",
                "doc_counterparty_inn",
                "doc_counterparty_full_name",
                "doc_provider_inn",
                "doc_provider_full_name",
                "doc_assigned_manager",
                "doc_department",
                "inside_doc_author",
                "doc_inside_assigned_manager",
                "inside_doc_type",
                "inside_doc_item_full_doc_price",
                "sn_cash_register",
                "name_cash_register",
                "doc_tariff",
                "end_data_fn",
                "quantity_of_cash_register",

                # "inside_doc_sale_number",
                # "inside_doc_sale_id",
                # "inside_doc_sale_type",
                
                "inside_doc_adress",
                "inside_doc_manufacture_year",
                "inside_doc_cash_sn",
                "inside_doc_model_cash",
                "inside_doc_quantity_of_cash_register",
                
            ], data= zip(
                lst_doc_id,
                lst_doc_type,
                lst_doc_number,
                lst_doc_full_name,
                lst_doc_data_main,
                lst_doc_at_created,
                lst_doc_counterparty_inn,
                lst_doc_counterparty_full_name,
                lst_doc_provider_inn,
                lst_doc_provider_full_name,
                lst_doc_assigned_manager,
                lst_doc_department,
                lst_inside_doc_author,
                lst_doc_inside_assigned_manager,
                lst_inside_doc_type,
                lst_inside_doc_item_full_doc_price,
                lst_sn_cash_register,
                lst_name_cash_register,
                lst_doc_tariff,
                lst_end_data_fn,
                lst_quantity_of_cash_register,        

                # lst_sale_number,
                # lst_sale_id,
                # lst_sale_type,
                
                lst_adress,
                lst_manufacture_year,
                lst_cash_sn,
                lst_model_cash,
                lst_inside_doc_quantity_of_cash_register,
            

            ))

            my_conn = create_engine(f"postgresql+psycopg2://da:qa123@10.82.2.30:5432/{var_db_name}")
            try: 
                my_conn.connect()
                print('connect')
                my_conn = my_conn.connect()
                df_to.to_sql(name=f"df_to_{var_name_date_for_upl}", schema=f"{var_to_docs_upl}" , con=my_conn, if_exists='replace')
                print('success!')
                my_conn.close()
            except:
                print('failed')


        # ________________________________________________________
        report_date = datetime.now().date()
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
                        var_name_date_for_upl = datetime(i_year,i_month,1).date().strftime('%Y%m%d')
                        start_date = datetime(i_year, i_month, 1).date().strftime('%d.%m.%Y')
                        end_date = (datetime(i_year, i_month, 1).date() + relativedelta(months=1, days=-1)).strftime('%d.%m.%Y')
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
                        var_name_date_for_upl = datetime(i_year,i_month,1).date().strftime('%Y%m%d')
                        start_date = datetime(i_year, i_month, 1).date().strftime('%d.%m.%Y')
                        end_date = (datetime(i_year, i_month, 1).date() + relativedelta(months=1, days=-1)).strftime('%d.%m.%Y')
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
                    var_name_date_for_upl = datetime(i_year,i_month,1).date().strftime('%Y%m%d')
                    start_date = datetime(i_year, i_month, 1).date().strftime('%d.%m.%Y')
                    end_date = (datetime(i_year, i_month, 1).date() + relativedelta(months=1, days=-1)).strftime('%d.%m.%Y')
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